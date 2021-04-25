const os = require("os");
const path = require("path");
const {MapReaderBase} = require("../lib/map-reader-base");
const bounds = require("binary-search-bounds");
const {program} = require('commander');
const {promises: fsp} = require("fs");
const fs = require("fs");
const crypto = require("crypto");
const OSM_NODE = 0;
const OSM_WAY = 1;
const OSM_RELATION = 2;
const MAX_ID_VALUE = 2 ** 48 - 1; // This thing uses unsigned int48s for indexing
const INT48_SIZE = 6;
const ELEMENT_INDEX_MAGIC_SIZE = 23;
const ELEMENT_INDEX_CHECKSUM_SIZE = 64;
const ELEMENT_INDEX_OFFSETS_START = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE;

const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option("--no-sanity-check", "Skip file validation")
	.option("--no-elem-index", "Do not create element index")
	.option("--no-parent-index", "Do not create element parent index")
	.parse()
	.opts();
const mapPath = path.resolve(options.map);
const mapReader = new MapReaderBase(mapPath);

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

const unpackIndexFile = async function(mapPath){
	const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
	const indexFileData = await fsp.readFile(path.resolve(mapPath, "..", mapName + ".neonmaps.element_index"));
	/**@type {Array<number>} */
	const nodeIDs = [];
	/**@type {Array<number>} */
	const wayIDs = [];
	/**@type {Array<number>} */
	const relationIDs = [];
	/**@type {Array<number>} */
	const nodeOffsets = [];
	/**@type {Array<number>} */
	const wayOffsets = [];
	/**@type {Array<number>} */
	const relationOffsets = [];
	let fileOffset = ELEMENT_INDEX_OFFSETS_START;
	const nodeIndexOffset = indexFileData.readUIntLE(fileOffset, INT48_SIZE);
	fileOffset += INT48_SIZE;
	const wayIndexOffset = indexFileData.readUIntLE(fileOffset, INT48_SIZE);
	fileOffset += INT48_SIZE;
	const relationIndexOffset = indexFileData.readUIntLE(fileOffset, INT48_SIZE);
	fileOffset += INT48_SIZE;
	const endIndexOffset = indexFileData.readUIntLE(fileOffset, INT48_SIZE);
	for(fileOffset = nodeIndexOffset; fileOffset < wayIndexOffset; fileOffset += INT48_SIZE){
		nodeIDs.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
		fileOffset += INT48_SIZE;
		nodeOffsets.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
	}
	for(fileOffset = wayIndexOffset; fileOffset < relationIndexOffset; fileOffset += INT48_SIZE){
		wayIDs.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
		fileOffset += INT48_SIZE;
		wayOffsets.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
	}
	for(fileOffset = relationIndexOffset; fileOffset < endIndexOffset; fileOffset += INT48_SIZE){
		relationIDs.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
		fileOffset += INT48_SIZE;
		relationOffsets.push(indexFileData.readUIntLE(fileOffset, INT48_SIZE));
	}
	return {
		nodeOffsets,
		wayOffsets,
		relationOffsets,
		nodeIDs,
		wayIDs,
		relationIDs
	};
}
const writeAndWait = async function(/**@type {fs.WriteStream}*/ stream, /**@type {Buffer}*/ data){
	if(!stream.write(data)){
		return new Promise(resolve => stream.once("drain", resolve));
	}
}
const parentIndex = async function(mapPath, mapSize, tmpDir, fileOffset, mapFileHashPromise){
	let relativeFileOffset = 0;
	const relativeEndOffset = mapSize - fileOffset;
	const indexData = await unpackIndexFile(mapPath);

	console.log("Element parent mapping: 0/" + relativeEndOffset + " (0%)");
	/**@type {Map<number, Array<Array<number>>} */
	const nodeToWayOffsetMap = new Map();
	/**@type {Map<number, Array<Array<number>>} */
	const nodeToRelationOffsetMap = new Map();
	/**@type {Map<number, Array<Array<number>>} */
	const wayToRelationOffsetMap = new Map();
	/**@type {Map<number, Array<Array<number>>} */
	const relationToRelationOffsetMap = new Map();

	
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
			/**@type {Map<number, Array<number>} */
			const nodeToWayMap = new Map();
			/**@type {Map<number, Array<number>} */
			const nodeToRelationMap = new Map();
			/**@type {Map<number, Array<number>} */
			const wayToRelationMap = new Map();
			/**@type {Map<number, Array<number>} */
			const relationToRelationMap = new Map();
			for(let i = 0; i < rawData.primitivegroup.length; i += 1){
				const group = rawData.primitivegroup[i];
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
				}
			}
			let smallestID = Infinity;
			let largestID = 0;
			let parentOffsetData;
			const childOffsets = new Set();
			let smallestChildID = Infinity;
			let largestChildID = 0;
			let smallestOffsetChildID = Infinity;
			let largestOffsetChildID = 0;
			let currentChildOffset = 0;
			if(nodeToWayMap.size){
				for(const [childID, parentIDs] of nodeToWayMap){
					if(childID > largestChildID){
						largestChildID = childID;
					}
					if(childID < smallestChildID){
						smallestChildID = childID;
					}
					parentIDs.forEach(parentID => {
						if(smallestID > parentID){
							smallestID = parentID;
						}
						if(largestID < parentID){
							largestID = parentID;
						}
					});
					if(childID > largestOffsetChildID || childID < smallestOffsetChildID){
						// Only search for the offset if we have to
						const indexIndex = bounds.le(indexData.nodeIDs, childID);
						smallestOffsetChildID = indexData.nodeIDs[indexIndex];
						currentChildOffset = indexData.nodeOffsets[indexIndex];
						if(indexIndex >= indexData.nodeIDs.length){
							largestOffsetChildID = Infinity;
						}else{
							largestOffsetChildID = indexData.nodeIDs[indexIndex];
						}
					}
					childOffsets.add(currentChildOffset);
				}
				parentOffsetData = [smallestChildID, largestChildID, fileOffset, smallestID, largestID];
				childOffsets.forEach(childOffset => {
					if(nodeToWayOffsetMap.has(childOffset)){
						nodeToWayOffsetMap.get(childOffset).push(parentOffsetData);
					}else{
						nodeToWayOffsetMap.set(childOffset, [parentOffsetData]);
					}
				});
				smallestID = Infinity;
				largestID = 0;
				childOffsets.clear();
				smallestChildID = Infinity;
				largestChildID = 0;
				smallestOffsetChildID = Infinity;
				largestOffsetChildID = 0;
				currentChildOffset = 0;
			}
			if(nodeToRelationMap.size){
				for(const [childID, parentIDs] of nodeToRelationMap){
					if(childID > largestChildID){
						largestChildID = childID;
					}
					if(childID < smallestChildID){
						smallestChildID = childID;
					}
					parentIDs.forEach(parentID => {
						if(smallestID > parentID){
							smallestID = parentID;
						}
						if(largestID < parentID){
							largestID = parentID;
						}
					});
					if(childID > largestOffsetChildID || childID < smallestOffsetChildID){
						// Only search for the offset if we have to
						const indexIndex = bounds.le(indexData.nodeIDs, childID);
						smallestOffsetChildID = indexData.nodeIDs[indexIndex];
						currentChildOffset = indexData.nodeOffsets[indexIndex];
						if(indexIndex >= indexData.nodeIDs.length){
							largestOffsetChildID = Infinity;
						}else{
							largestOffsetChildID = indexData.nodeIDs[indexIndex];
						}
					}
					childOffsets.add(currentChildOffset);
				}
				parentOffsetData = [smallestChildID, largestChildID, fileOffset, smallestID, largestID];
				childOffsets.forEach(childOffset => {
					if(nodeToRelationOffsetMap.has(childOffset)){
						// NOTE: this will already be sorted by ID
						nodeToRelationOffsetMap.get(childOffset).push(parentOffsetData);
					}else{
						nodeToRelationOffsetMap.set(childOffset, [parentOffsetData]);
					}
				});
				smallestID = Infinity;
				largestID = 0;
				childOffsets.clear();
				smallestChildID = Infinity;
				largestChildID = 0;
				smallestOffsetChildID = Infinity;
				largestOffsetChildID = 0;
				currentChildOffset = 0;
			}
			if(wayToRelationMap.size){
				for(const [childID, parentIDs] of wayToRelationMap){
					if(childID > largestChildID){
						largestChildID = childID;
					}
					if(childID < smallestChildID){
						smallestChildID = childID;
					}
					parentIDs.forEach(parentID => {
						if(smallestID > parentID){
							smallestID = parentID;
						}
						if(largestID < parentID){
							largestID = parentID;
						}
					});
					if(childID > largestOffsetChildID || childID < smallestOffsetChildID){
						// Only search for the offset if we have to
						const indexIndex = bounds.le(indexData.wayIDs, childID);
						smallestOffsetChildID = indexData.wayIDs[indexIndex];
						currentChildOffset = indexData.wayOffsets[indexIndex];
						if(indexIndex >= indexData.wayIDs.length){
							largestOffsetChildID = Infinity;
						}else{
							largestOffsetChildID = indexData.wayIDs[indexIndex];
						}
					}
					childOffsets.add(currentChildOffset);
				}
				parentOffsetData = [smallestChildID, largestChildID, fileOffset, smallestID, largestID];
				childOffsets.forEach(childOffset => {
					if(wayToRelationOffsetMap.has(childOffset)){
						// NOTE: this will already be sorted by ID
						wayToRelationOffsetMap.get(childOffset).push(parentOffsetData);
					}else{
						wayToRelationOffsetMap.set(childOffset, [parentOffsetData]);
					}
				});
				smallestID = Infinity;
				largestID = 0;
				childOffsets.clear();
				smallestChildID = Infinity;
				largestChildID = 0;
				smallestOffsetChildID = Infinity;
				largestOffsetChildID = 0;
				currentChildOffset = 0;
			}
			if(relationToRelationMap.size){
				for(const [childID, parentIDs] of relationToRelationMap){
					if(childID > largestChildID){
						largestChildID = childID;
					}
					if(childID < smallestChildID){
						smallestChildID = childID;
					}
					parentIDs.forEach(parentID => {
						if(smallestID > parentID){
							smallestID = parentID;
						}
						if(largestID < parentID){
							largestID = parentID;
						}
					});
					if(childID > largestOffsetChildID || childID < smallestOffsetChildID){
						// Only search for the offset if we have to
						const indexIndex = bounds.le(indexData.relationIDs, childID);
						smallestOffsetChildID = indexData.relationIDs[indexIndex];
						currentChildOffset = indexData.relationOffsets[indexIndex];
						if(indexIndex >= indexData.relationIDs.length){
							largestOffsetChildID = Infinity;
						}else{
							largestOffsetChildID = indexData.relationIDs[indexIndex];
						}
					}
					childOffsets.add(currentChildOffset);
				}
				parentOffsetData = [smallestChildID, largestChildID, fileOffset, smallestID, largestID];
				childOffsets.forEach(childOffset => {
					if(relationToRelationOffsetMap.has(childOffset)){
						// NOTE: this will already be sorted by ID
						relationToRelationOffsetMap.get(childOffset).push(parentOffsetData);
					}else{
						relationToRelationOffsetMap.set(childOffset, [parentOffsetData]);
					}
				});
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
	let parentLengthByteLength = 0;
	const mapForEachFunc = (/**@type {Array<Array<number>>} */ data) => {
		data.sort((a, b) => a[0] - b[0]);
		if(data.length > parentLengthByteLength){
			parentLengthByteLength = data.length;
		}
	};
	nodeToWayOffsetMap.forEach(mapForEachFunc);
	nodeToRelationOffsetMap.forEach(mapForEachFunc);
	wayToRelationOffsetMap.forEach(mapForEachFunc);
	relationToRelationOffsetMap.forEach(mapForEachFunc);
	parentLengthByteLength = Math.ceil(Math.ceil(Math.log2(parentLengthByteLength + 1)) / 8);

	console.log("Element parent mapping: " + relativeEndOffset + "/" + relativeEndOffset + " (100%)");
	const parentsToWrite = nodeToWayOffsetMap.size + nodeToRelationOffsetMap.size + wayToRelationOffsetMap.size +
		relationToRelationOffsetMap.size;
	let parentsWritten = 0;
	/**@type {Buffer} */
	const fileHash = await mapFileHashPromise;
	console.log("Element parent file writing: 0/" + parentsToWrite + " (0%)");
	const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
	const indexFileStream = fs.createWriteStream(path.resolve(mapPath, "..", mapName + ".neonmaps.parent_index"));
	const tmpParentListPath = tmpDir + path.sep + "parent_list";
	const tmpParentListStream = fs.createWriteStream(tmpParentListPath);

	const magic = "neonmaps.parent_index\0";
	let listOffset = magic.length + fileHash.length + 1 + parentsToWrite * 2 * INT48_SIZE + 4 * INT48_SIZE;
	indexFileStream.write(magic);
	indexFileStream.write(fileHash);
	indexFileStream.write(Buffer.alloc(1, parentLengthByteLength));
	const headerDataBuf = Buffer.allocUnsafe(INT48_SIZE * 4)
	headerDataBuf.writeIntLE(nodeToWayOffsetMap.size, 0, INT48_SIZE);
	headerDataBuf.writeIntLE(nodeToRelationOffsetMap.size, INT48_SIZE, INT48_SIZE);
	headerDataBuf.writeIntLE(wayToRelationOffsetMap.size, INT48_SIZE * 2, INT48_SIZE);
	headerDataBuf.writeIntLE(relationToRelationOffsetMap.size, INT48_SIZE * 3, INT48_SIZE);
	indexFileStream.write(headerDataBuf);
	const writeStuff = async (/**@type {Map<number, Array<Array<number>>>} */ childToParentMap) => {
		parentsWritten += 1;
		const childOffsets = [...childToParentMap.keys()];
		childOffsets.sort((a, b) => a - b);
		for(let i = 0; i < childOffsets.length; i += 1){
			parentsWritten += 1;
			const indexBuf = Buffer.allocUnsafe(INT48_SIZE * 2);
			indexBuf.writeUIntLE(childOffsets[i], 0, INT48_SIZE);
			indexBuf.writeUIntLE(listOffset, INT48_SIZE, INT48_SIZE);
			await writeAndWait(indexFileStream, indexBuf);

			const parentData = childToParentMap.get(childOffsets[i]);
			const listLengthBuf = Buffer.allocUnsafe(parentLengthByteLength);
			listLengthBuf.writeUIntLE(parentData.length, 0, parentLengthByteLength);
			await writeAndWait(tmpParentListStream, listLengthBuf);
			listOffset += listLengthBuf.length;
			for(let ii = 0; ii < parentData.length; ii += 1){
				const listBuf = Buffer.allocUnsafe(INT48_SIZE * parentData[ii].length);
				parentData[ii].forEach((val, iii) => listBuf.writeUIntLE(val, iii * INT48_SIZE, INT48_SIZE));
				await writeAndWait(tmpParentListStream, listBuf);
				listOffset += listBuf.length;
			}
			logProgressMsg(
				"Element parent file writing: " + parentsWritten + "/" + parentsToWrite + " (" +
				(parentsWritten / parentsToWrite * 100).toFixed(2) +
				"%)"
			);
		}
	};
	await writeStuff(nodeToWayOffsetMap);
	await writeStuff(nodeToRelationOffsetMap);
	await writeStuff(wayToRelationOffsetMap);
	await writeStuff(relationToRelationOffsetMap);
	const tmpFileClose = new Promise(resolve => {
		tmpParentListStream.once("close", resolve);
	});
	tmpParentListStream.end();
	await tmpFileClose;
	fs.createReadStream(tmpParentListPath).pipe(indexFileStream);
	await new Promise(resolve => {
		indexFileStream.once("close", resolve);
	});
	console.log("Element parent file writing: " + parentsToWrite + "/" + parentsToWrite + " (100%)");
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
		/**@type {Promise<Buffer>} */
		const mapFileHashPromise = new Promise((resolve, reject) => {
			const hasher = crypto.createHash("sha512");
			const mapFileStream = fs.createReadStream(mapPath);
			mapFileStream.once("error", reject);
			mapFileStream.on("data", c => {hasher.update(c);});
			mapFileStream.once("end", () => resolve(hasher.digest()));
		});
		if(options.elemIndex){
			const nodeIndexStream = fs.createWriteStream(path.resolve(tmpDir, "nodes"));
			const wayIndexStream = fs.createWriteStream(path.resolve(tmpDir, "ways"));
			const relationIndexStream = fs.createWriteStream(path.resolve(tmpDir, "relations"));
			let nodeIndexSize = 0;
			let wayIndexSize = 0;
			let relationIndexSize = 0;
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
			await parentIndex(mapPath, mapSize, tmpDir, firstOffsetWithNonNode, mapFileHashPromise);
		}
		await fsp.rm(tmpDir, {recursive: true, force: true});
		await mapReader.stop();
	}catch(ex){
		process.exitCode = 1;
		console.error(ex);
	}
})();
