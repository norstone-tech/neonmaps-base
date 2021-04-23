const os = require("os");
const path = require("path");
const {MapReaderBase} = require("../lib/map-reader-base");
const {NumericIndexFileSearcher} = require("../lib/index-file-searcher");
const {CachedFileHandle} = require("../lib/cached-file-handle");
const {program} = require('commander');
const {promises: fsp} = require("fs");
const fs = require("fs");
const crypto = require("crypto");
const bounds = require('binary-search-bounds');
const OSM_NODE = 0;
const OSM_WAY = 1;
const OSM_RELATION = 2;
const MAX_ID_VALUE = 2 ** 48 - 1; // This thing uses unsigned int48s for indexing
const INT48_SIZE = 6;
const NEW_PARENT_LIST_COUNT = 5;
const NEW_PARENT_LIST_SIZE = INT48_SIZE * NEW_PARENT_LIST_COUNT;
const PARENT_INDEX_BLOCK_SIZE = INT48_SIZE * 2;
const PARENT_INDEXER_INSERT_CHUNK = 512 * 1024 * 1024;

const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option("--no-sanity-check", "Skip file validation")
	.option("--no-elem-index", "Do not create element index")
	.option("--no-parent-index", "Do not create element parent index")
	.parse()
	.opts();
const mapPath = path.resolve(options.map);
const mapReader = new MapReaderBase(mapPath);
class ElementParentIndexer {
	constructor(path){
		this.parentFilePath = path;
		this.indexFilePath = path + "_index";
		this.parentFileSize = 0;
		this.indexFileSize = 0;
		/**@type {Array<number>} */
		this.emptyParentSpaces = [];

		this.indexFile = new CachedFileHandle(this.parentFilePath, "w+", 1048576);
		this.parentFile = new CachedFileHandle(this.indexFilePath, "w+", 1048576);
	}
	async init(){
		await Promise.all([
			this.indexFile.open(),
			this.parentFile.open()
		]);
	}
	async stop(){
		await Promise.all([
			this.parentFile.close(),
			this.indexFile.close()
		]);
	}
	async growIndexFile(size = 0){
		if(!size){return;}
		await this.indexFile.write(this.indexFileSize, Buffer.alloc(size));
		this.indexFileSize += size;
		this.indexFileSearcher = new NumericIndexFileSearcher(this.indexFile, 12, INT48_SIZE, 0, 0, this.indexFileSize);
		await this.indexFileSearcher.init();
	}
	async growParentFile(size = 0){
		await this.parentFile.write(this.parentFileSize, Buffer.alloc(size));
		this.parentFileSize += size;
	}
	async findEmptySpaceInParentFile(size = NEW_PARENT_LIST_SIZE){
		if((size % NEW_PARENT_LIST_SIZE) !== 0){
			throw new Error("This shouldn't happen");
		}
		const consecutiveRequired = size / NEW_PARENT_LIST_SIZE;
		if(this.emptyParentSpaces.length < consecutiveRequired){
			const result = this.parentFileSize;
			await this.growParentFile(size);
			return result;
		}
		if(consecutiveRequired == 1){
			return this.emptyParentSpaces.shift()
		}
		let consecutiveFound = 0;
		for(let i = 1; i < this.emptyParentSpaces.length; i += 1){
			const prevOffset = this.emptyParentSpaces[i - 1];
			const curOffset = this.emptyParentSpaces[i];
			if((curOffset - prevOffset) == NEW_PARENT_LIST_SIZE){
				consecutiveFound += 1;
				if(consecutiveFound == consecutiveRequired){
					return this.emptyParentSpaces.splice(i - consecutiveFound, consecutiveFound)[0];
				}
			}
		}
		const result = this.parentFileSize;
		await this.growParentFile(size);
		return result;
	}
	async findOffsetToWriteParentIDIn(id, amount){
		const newFreeSpaceRequired = (
			(amount + 1) - ((amount + 1) % NEW_PARENT_LIST_COUNT) + NEW_PARENT_LIST_COUNT
		) * INT48_SIZE;
		const intBuf = Buffer.allocUnsafe(INT48_SIZE);
		if(this.parentFileSize === 0){
			await Promise.all([
				this.growIndexFile(PARENT_INDEX_BLOCK_SIZE),
				this.growParentFile(newFreeSpaceRequired)
			]);
			intBuf.writeUIntLE(id, 0, INT48_SIZE);
			await this.indexFile.write(0, intBuf);
			return 0;
		}
		const indexIndex = await this.indexFileSearcher.ge(id);
		if(indexIndex == this.indexFileSearcher.length){
			console.time("append at end");
			// this ID is larger than any in existance, append at the end
			let indexOffset = this.indexFileSize;
			const [result] = await Promise.all([
				this.findEmptySpaceInParentFile(newFreeSpaceRequired),
				this.growIndexFile(PARENT_INDEX_BLOCK_SIZE)
			]);
			intBuf.writeUIntLE(id, 0, INT48_SIZE);
			await this.indexFile.write(indexOffset, intBuf);
			indexOffset += 6;
			intBuf.writeUIntLE(result, 0, INT48_SIZE);
			await this.indexFile.write(indexOffset, intBuf);
			console.timeEnd("append at end");
			return result;
		}
		if((await this.indexFileSearcher.item(indexIndex)).readUIntLE(0, INT48_SIZE) !== id){
			// ID must be inserted at the found index in order for this thing to remain sorted
			//console.time("insertion");
			const oldSize = this.indexFileSize;
			let indexOffset = indexIndex * 12;
			const [result] = await Promise.all([
				this.findEmptySpaceInParentFile(newFreeSpaceRequired),
				this.growIndexFile(PARENT_INDEX_BLOCK_SIZE)
			]);

			const stuffToMove = oldSize - indexOffset;
			const insertRemainderChunk = stuffToMove % PARENT_INDEXER_INSERT_CHUNK;
			for(let i = oldSize - PARENT_INDEXER_INSERT_CHUNK; i >= indexOffset; i -= 12){
				await this.indexFile.write(
					i + PARENT_INDEX_BLOCK_SIZE,
					await this.indexFile.read(i, PARENT_INDEXER_INSERT_CHUNK)
				);
			}
			if(insertRemainderChunk > 0){
				await this.indexFile.write(
					indexOffset + PARENT_INDEX_BLOCK_SIZE,
					await this.indexFile.read(indexOffset, insertRemainderChunk)
				);
			}
			intBuf.writeUIntLE(id, 0, INT48_SIZE);
			await this.indexFile.write(indexOffset, intBuf);
			indexOffset += INT48_SIZE;
			intBuf.writeUIntLE(result, 0, INT48_SIZE);
			await this.indexFile.write(indexOffset, intBuf);
			//console.timeEnd("insertion");
			return result;
		}


		let startOffset = (await this.indexFileSearcher.item(indexIndex)).readUIntLE(INT48_SIZE, INT48_SIZE);
		let offset = startOffset;
		let curListLength = 0;
		// Find end of list
		while((await this.parentFile.read(offset, INT48_SIZE)).readUIntLE(0, INT48_SIZE) !== 0){
			curListLength += 1;
			offset += INT48_SIZE;
		}
		offset += INT48_SIZE;
		if((offset + INT48_SIZE) >= this.parentFileSize){
			// We've reached the end of the file, just grow it
			await this.growParentFile(newFreeSpaceRequired);
			return offset;
		}
		// Check if we have enough space to append
		let enoughSpace = true;
		for(let i = 0; i <= amount; i += 1){
			if((await this.parentFile.read(offset + i * INT48_SIZE, INT48_SIZE)).readUIntLE(0, INT48_SIZE) !== 0){
				enoughSpace = false;
				break;
			}
		}
		if(enoughSpace){
			// Make sure the space we're using isn't marked as "free"
			for(let i = 0; i <= amount; i += 1){
				const ii = bounds.eq(this.emptyParentSpaces, offset + i * INT48_SIZE);
				if(ii != -1){
					this.emptyParentSpaces.splice(ii, 1);
				}
			}
			return offset;
		}

		console.time("reallocate");
		const spaceRequired = offset - startOffset - INT48_SIZE + newFreeSpaceRequired;
		const newOffset = this.findEmptySpaceInParentFile(spaceRequired);
		offset -= INT48_SIZE; // Last number is 0, we don't need to copy that
		const parentListBuffer = await this.parentFile.read(startOffset, offset);
		// Clear where the old parent list used to be
		await this.parentFile.write(startOffset, Buffer.alloc(offset - startOffset));
		for(let i = startOffset; i < offset; offset += PARENT_INDEX_BLOCK_SIZE){
			this.emptyParentSpaces.push(i);
		}
		this.emptyParentSpaces.sort();
		// Remember to update the index file!
		intBuf.writeUIntLE(newOffset, 0, INT48_SIZE);
		await Promise.all([
			this.indexFile.write(indexIndex * 12 + INT48_SIZE, intBuf),
			this.parentFile.write(newOffset, parentListBuffer)
		]);
		console.timeEnd("reallocate");
		return curListLength * INT48_SIZE + newOffset;
	}
	async writeMap(/**@type {Map<number, Array<number>}*/ map){
		let writeAmount = 0;
		for(const id of map.keys()){
			const parents = map.get(id);
			const parentsBuf = Buffer.allocUnsafe(INT48_SIZE * parents.length);
			for(let i = 0; i < parents.length; i += 1){
				parentsBuf.writeUIntLE(parents[i], i * INT48_SIZE, INT48_SIZE);
			}
			await this.parentFile.write(await this.findOffsetToWriteParentIDIn(id, parents.length), parentsBuf);
			writeAmount += 1;
			// console.log("ElementParentIndexer.writeMap:", writeAmount, "/", map.size);
		}
	}
}
let nextProgressMsg = Date.now();

const logProgressMsg = function(...msg){
	if(nextProgressMsg <= Date.now()){
		console.log(...msg);
		nextProgressMsg = Date.now() + 300;
	}
}

const sanityCheck = async function(){
	console.log("Ensuring all IDs are increasing...");
	const {size: mapSize} = await fsp.stat(mapPath);
	const mapHeader = await mapReader.readMapSegment(0);
	let fileOffset = mapHeader._byte_size;
	let lastNodeId = 0;
	let lastWayId = 0;
	let lastRelationId = 0;
	let segmentCount = 0;
	while(fileOffset < mapSize){
		/**@type {import("../lib/map-reader-base").OSMData} */
		const rawData = await mapReader.readMapSegment(fileOffset);
		const mapSegment = MapReaderBase.decodeRawData(rawData);
		for(let i = 0; i < mapSegment.nodes.length; i += 1){
			const node = mapSegment.nodes[i];
			if(lastNodeId >= node.id){
				throw new Error("Unordered node at offset " + fileOffset + " index " + i);
			}
			lastNodeId = node.id;
			if(lastNodeId > MAX_ID_VALUE){
				throw new Error("Node ID greater than maximum " + MAX_ID_VALUE + " at offset " + fileOffset);
			}
		}
		for(let i = 0; i < mapSegment.ways.length; i += 1){
			const way = mapSegment.ways[i];
			if(lastWayId >= way.id){
				throw new Error("Unordered way at offset " + fileOffset + " index " + i);
			}
			lastWayId = way.id;
			if(lastWayId > MAX_ID_VALUE){
				throw new Error("Way ID greater than maximum " + MAX_ID_VALUE + " at offset " + fileOffset);
			}
		}
		for(let i = 0; i < mapSegment.relations.length; i += 1){
			const relation = mapSegment.relations[i];
			if(lastRelationId >= relation.id){
				throw new Error("Unordered relation at offset " + fileOffset + " index " + i);
			}
			lastRelationId = relation.id;
			if(lastRelationId > MAX_ID_VALUE){
				throw new Error("Relation ID greater than maximum " + MAX_ID_VALUE + " at offset " + fileOffset);
			}
		}
		fileOffset += rawData._byte_size;
		segmentCount += 1;
		/*
		console.log({
			segmentCount,
			lastNodeId,
			lastWayId,
			lastRelationId
		});
		*/
		logProgressMsg(
			"File verification: " + fileOffset + "/" + mapSize + " (" +
			(fileOffset / mapSize * 100).toFixed(2) +
			"%)"
		);
	}
	console.log("File verification: " + fileOffset + "/" + mapSize + " (100%)");
};
const parentIndex = async function(mapSize, tmpDir, fileOffset){
	let relativeFileOffset = 0;
	const relativeEndOffset = mapSize - fileOffset;
	const nodeToWayIndexer = new ElementParentIndexer(tmpDir + path.sep + "node_to_way");
	const nodeToRelationIndexer = new ElementParentIndexer(tmpDir + path.sep + "node_to_relation");
	const wayToRelationIndexer = new ElementParentIndexer(tmpDir + path.sep + "way_to_relation");
	const relationToRelationIndexer = new ElementParentIndexer(tmpDir + path.sep + "relation_to_relation");
	console.log("Element parent mapping: 0/" + relativeEndOffset + " (0%)");
	await Promise.all([
		nodeToWayIndexer.init(),
		nodeToRelationIndexer.init(),
		wayToRelationIndexer.init(),
		relationToRelationIndexer.init()
	]);

	// I could do this in a single while loop to save time, but this is fine for now
	while(fileOffset < mapSize){
		/**@type {import("../lib/map-reader-base").OSMData} */
		const rawData = await mapReader.readMapSegment(fileOffset);
		let skip = true;
		for(let i = 0; i < rawData.primitivegroup.length; i += 1){
			const group = rawData.primitivegroup[i];
			if(group.ways.length > 0 || group.relations.length > 0){
				skip = false;
				break;
			}
		}
		if(!skip){
			for(let i = 0; i < rawData.primitivegroup.length; i += 1){
				const group = rawData.primitivegroup[i];
				/**@type {Map<number, Array<number>} */
				const nodeToWayMap = new Map();
				for(let ii = 0; ii < group.ways.length; ii += 1){
					// console.log("ways: ", ii, "/", group.ways.length);
					const way = group.ways[ii];
					let nodeId = 0;
					for(let iii = 0; iii < way.refs.length; iii += 1){
						nodeId += way.refs[iii];
						if(nodeToWayMap.has(nodeId)){
							nodeToWayMap.get(nodeId).push(way.id);
						}else{
							nodeToWayMap.set(nodeId, [way.id]);
						}
					}
				}
				await nodeToWayIndexer.writeMap(nodeToWayMap);
				if(group.ways.length){
					// If this thing contained ways, then it doesn't contain any relations
					continue;
				}
				/**@type {Map<number, Array<number>} */
				const nodeToRelationMap = new Map();
				/**@type {Map<number, Array<number>} */
				const wayToRelationMap = new Map();
				/**@type {Map<number, Array<number>} */
				const relationToRelationMap = new Map();
				for(let ii = 0; ii < group.relations.length; ii += 1){
					const relation = group.relations[ii];
					let memId = 0;
					for(let iii = 0; iii < relation.memids.length; iii += 1){
						memId += relation.memids[iii];
						switch(relation.types[iii]){
							case OSM_NODE:
								if(nodeToRelationMap.has(memId)){
									nodeToRelationMap.get(memId).push(relation.id);
								}else{
									nodeToRelationMap.set(memId, [relation.id]);
								}
								break;
							case OSM_WAY:
								if(wayToRelationMap.has(memId)){
									wayToRelationMap.get(memId).push(relation.id);
								}else{
									wayToRelationMap.set(memId, [relation.id]);
								}
								break;
							case OSM_RELATION:
								if(relationToRelationMap.has(memId)){
									relationToRelationMap.get(memId).push(relation.id);
								}else{
									relationToRelationMap.set(memId, [relation.id]);
								}
								break;
						}
					}
					// relation.memids
				}
				if(nodeToRelationMap.size){
					await nodeToRelationIndexer.writeMap(nodeToRelationMap);
				}
				if(wayToRelationMap.size){
					await nodeToRelationIndexer.writeMap(wayToRelationMap);
				}
				if(relationToRelationMap.size){
					await nodeToRelationIndexer.writeMap(relationToRelationMap);
				}
			}


		}
		fileOffset += rawData._byte_size;
		relativeFileOffset += rawData._byte_size;
		logProgressMsg(
			"Element parent mapping: " + relativeFileOffset + "/" + relativeEndOffset + " (" +
			(relativeFileOffset / relativeEndOffset * 100).toFixed(2) +
			"%)"
		);
	}
	await Promise.all([
		nodeToWayIndexer.stop(),
		nodeToRelationIndexer.stop(),
		wayToRelationIndexer.stop(),
		relationToRelationIndexer.stop()
	]);
	console.log("Element parent mapping: " + relativeEndOffset + "/" + relativeEndOffset + " (100%)");
};
(async() => {
	try{
		const {size: mapSize} = await fsp.stat(mapPath);
		await mapReader.init();
		if(options.sanityCheck){
			await sanityCheck();
		}
		const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "neonmaps-indexer-"));
		let fileOffset = (await mapReader.readMapSegment(0))._byte_size;
		let firstOffsetWithNonNode = 0;
		if(options.elemIndex){
			const nodeIndexStream = fs.createWriteStream(path.resolve(tmpDir, "nodes"));
			const wayIndexStream = fs.createWriteStream(path.resolve(tmpDir, "ways"));
			const relationIndexStream = fs.createWriteStream(path.resolve(tmpDir, "relations"));
			let nodeIndexSize = 0;
			let wayIndexSize = 0;
			let relationIndexSize = 0;
			/**@type {Promise<Buffer>} */
			const mapFileHashPromise = new Promise((resolve, reject) => {
				const hasher = crypto.createHash("sha512");
				const mapFileStream = fs.createReadStream(mapPath);
				mapFileStream.once("error", reject);
				mapFileStream.on("data", c => {hasher.update(c);});
				mapFileStream.once("end", () => resolve(hasher.digest()));
			});
			console.log("Element ID indexing: " + fileOffset + "/" + mapSize + " (0%)");
			while(fileOffset < mapSize){
				/**@type {import("../lib/map-reader-base").OSMData} */
				const rawData = await mapReader.readMapSegment(fileOffset);
				for(let i = 0; i < rawData.primitivegroup.length; i += 1){
					const rawGroup = rawData.primitivegroup[i];
					const buf = Buffer.allocUnsafe(12); // 48 bits for ID, 48 bits for file offset
					/**@type {fs.WriteStream} */
					let indexStream;
					/* As said by the OSM wiki: A PrimitiveGroup MUST NEVER contain different types of objects. So
					   either it contains many Node messages, or a DenseNode message, or many Way messages, or many
					   Relation messages, or many ChangeSet messages. But it can never contain any mixture of those. */
					if(rawGroup.dense){
						buf.writeUIntLE(rawGroup.dense.id[0], 0, INT48_SIZE);
						nodeIndexSize += 12;
						indexStream = nodeIndexStream;
					}else if(rawGroup.nodes.length){
						buf.writeUIntLE(rawGroup.nodes[i].id, 0, INT48_SIZE);
						nodeIndexSize += 12;
						indexStream = nodeIndexStream;
					}else if(rawGroup.ways.length){
						buf.writeUIntLE(rawGroup.ways[i].id, 0, INT48_SIZE);
						wayIndexSize += 12;
						indexStream = wayIndexStream;
						if(!firstOffsetWithNonNode){
							firstOffsetWithNonNode = fileOffset;
						}
					}else if(rawGroup.relations.length){
						buf.writeUIntLE(rawGroup.relations[i].id, 0, INT48_SIZE);
						relationIndexSize += 12;
						indexStream = relationIndexStream;
						if(!firstOffsetWithNonNode){
							firstOffsetWithNonNode = fileOffset;
						}
					}else{
						// We don't give a shit about changesets right now
						continue;
					}
					buf.writeUIntLE(fileOffset, INT48_SIZE, INT48_SIZE);
					if(!indexStream.write(buf)){
						await new Promise(resolve => {
							indexStream.once("drain", resolve);
						});
					}
				}
				fileOffset += rawData._byte_size;
				logProgressMsg(
					"Element ID indexing: " + fileOffset + "/" + mapSize + " (" +
					(fileOffset / mapSize * 100).toFixed(2) +
					"%)"
				);
			}
			console.log("Element ID indexing: " + mapSize + "/" + mapSize + " (100%)");
			

			indexStreamClosePromise = Promise.all([
				new Promise(resolve => {
					nodeIndexStream.once("close", resolve);
				}),
				new Promise(resolve => {
					wayIndexStream.once("close", resolve);
				}),
				new Promise(resolve => {
					relationIndexStream.once("close", resolve);
				})
			]);
			nodeIndexStream.end();
			wayIndexStream.end();
			relationIndexStream.end();
			console.log("Element ID index stitching: 0/4 (0%)");
			const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
			const indexFileStream = fs.createWriteStream(path.resolve(mapPath, "..", mapName + ".neonmaps.element_index"));
			indexFileStream.write("neonmaps.element_index\0"); // NUL is the version number, which is now 0.
			indexFileStream.write(await mapFileHashPromise);

			let offsetNum = "neonmaps.element_index\0".length + 64 + 24; // 512 bits -> 64 bytes, plus 24 for the 4 6-byte values
			const offsetNumBuffer = Buffer.allocUnsafe(24);
			offsetNumBuffer.writeUIntLE(offsetNum, 0, INT48_SIZE);
			offsetNum += nodeIndexSize;
			offsetNumBuffer.writeUIntLE(offsetNum, INT48_SIZE, INT48_SIZE);
			offsetNum += wayIndexSize;
			offsetNumBuffer.writeUIntLE(offsetNum, 12, INT48_SIZE);
			offsetNum += relationIndexSize;
			offsetNumBuffer.writeUIntLE(offsetNum, 18, INT48_SIZE);
			indexFileStream.write(offsetNumBuffer);
			console.log("Element ID index stitching: 1/4 (25%)");
			let tmpStream = fs.createReadStream(path.resolve(tmpDir, "nodes"));
			tmpStream.pipe(indexFileStream, {end: false});
			await new Promise(resolve => tmpStream.on("close", resolve));
			console.log("Element ID index stitching: 2/4 (50%)");
			tmpStream = fs.createReadStream(path.resolve(tmpDir, "ways"));
			tmpStream.pipe(indexFileStream, {end: false});
			await new Promise(resolve => tmpStream.on("close", resolve));
			console.log("Element ID index stitching: 3/4 (75%)");
			tmpStream = fs.createReadStream(path.resolve(tmpDir, "relations"));
			tmpStream.pipe(indexFileStream);
			await new Promise(resolve => tmpStream.on("close", resolve));
			console.log("Element ID index stitching: 4/4 (100%)");
		}else{
			while(!firstOffsetWithNonNode){
				/**@type {import("../lib/map-reader-base").OSMData} */
				const rawData = await mapReader.readMapSegment(fileOffset);
				for(let i = 0; i < rawData.primitivegroup.length; i += 1){
					const rawGroup = rawData.primitivegroup[i];
					if(rawGroup.ways.length || rawGroup.relations.length){
						firstOffsetWithNonNode = fileOffset;
						break;
					}
				}
				fileOffset += rawData._byte_size;
				logProgressMsg(
					"Non-node search: " + fileOffset + "/" + mapSize + " (" +
					(fileOffset / mapSize * 100).toFixed(2) +
					"%)"
				);
			}
		}
		if(options.parentIndex){
			await parentIndex(mapSize, tmpDir, firstOffsetWithNonNode);
		}
		// await fsp.rm(tmpDir, {recursive: true, force: true});
		await mapReader.stop();
	}catch(ex){
		process.exitCode = 1;
		console.error(ex);
	}
})();
