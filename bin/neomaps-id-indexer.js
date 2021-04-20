const os = require("os");
const path = require("path");
const {MapReaderBase} = require("../lib/map-reader-base");
const {program} = require('commander');
const {promises: fsp} = require("fs");
const fs = require("fs");
const crypto = require("crypto");
const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option("--no-sanity-check", "Skip file validation")
	.parse()
	.opts();
const mapPath = path.resolve(options.map);
const mapReader = new MapReaderBase(mapPath);

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
		}
		for(let i = 0; i < mapSegment.ways.length; i += 1){
			const way = mapSegment.ways[i];
			if(lastWayId >= way.id){
				throw new Error("Unordered way at offset " + fileOffset + " index " + i);
			}
			lastWayId = way.id;
		}
		for(let i = 0; i < mapSegment.relations.length; i += 1){
			const relation = mapSegment.relations[i];
			if(lastRelationId >= relation.id){
				throw new Error("Unordered relation at offset " + fileOffset + " index " + i);
			}
			lastRelationId = relation.id;
		}
		fileOffset += rawData._byte_size;
		segmentCount += 1;
		console.log({
			segmentCount,
			lastNodeId,
			lastWayId,
			lastRelationId
		});
		console.log(
			"sanity: " + fileOffset + "/" + mapSize + " (" +
			(fileOffset / mapSize * 100).toFixed(2) +
			"%)"
		);
	}
};
(async() => {
	try{
		const {size: mapSize} = await fsp.stat(mapPath);
		await mapReader.init();
		if(options.sanityCheck){
			await sanityCheck();
		}
		const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "neomaps-indexer-"));
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
		let fileOffset = (await mapReader.readMapSegment(0))._byte_size;
		console.log("indexing: " + fileOffset + "/" + mapSize + " (0%)");
		while(fileOffset < mapSize){
			/**@type {import("../lib/map-reader-base").OSMData} */
			const rawData = await mapReader.readMapSegment(fileOffset);
			for(let i = 0; i < rawData.primitivegroup.length; i += 1){
				const rawGroup = rawData.primitivegroup[i];
				const buf = Buffer.allocUnsafe(12); // 48 bits for ID, 48 bits for file offset
				/**@type {fs.WriteStream} */
				let indexStream;
				/* As said by the OSM wiki: A PrimitiveGroup MUST NEVER contain different types of objects. So either
				   it contains many Node messages, or a DenseNode message, or many Way messages, or many Relation
				   messages, or many ChangeSet messages. But it can never contain any mixture of those. */
				if(rawGroup.dense){
					buf.writeUIntLE(rawGroup.dense.id[0], 0, 6);
					nodeIndexSize += 12;
					indexStream = nodeIndexStream;
				}else if(rawGroup.nodes.length){
					buf.writeUIntLE(rawGroup.nodes[i].id, 0, 6);
					nodeIndexSize += 12;
					indexStream = nodeIndexStream;
				}else if(rawGroup.ways.length){
					buf.writeUIntLE(rawGroup.ways[i].id, 0, 6);
					wayIndexSize += 12;
					indexStream = wayIndexStream;
				}else if(rawGroup.relations.length){
					buf.writeUIntLE(rawGroup.relations[i].id, 0, 6);
					relationIndexSize += 12;
					indexStream = relationIndexStream;
				}else{
					// We don't give a shit about changesets right now
					continue;
				}
				buf.writeUIntLE(fileOffset, 6, 6);
				if(!indexStream.write(buf)){
					await new Promise(resolve => {
						indexStream.once("drain", resolve);
					});
				}
			}
			fileOffset += rawData._byte_size;
			console.log(
				"indexing: " + fileOffset + "/" + mapSize + " (" +
				(fileOffset / mapSize * 100).toFixed(2) +
				"%)"
			);
		}
		console.log("stitching: 0/4 (0%)");
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
		const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
		const indexFileStream = fs.createWriteStream(path.resolve(mapPath, "..", mapName + ".neonmaps.element_index"));
		indexFileStream.write("neonmaps.element_index\0"); // NUL is the version number, which is now 0.
		indexFileStream.write(await mapFileHashPromise);

		let offsetNum = "neonmaps.element_index\0".length + 64 + 24; // 512 bits -> 64 bytes, plus 24 for the 4 6-byte values
		const offsetNumBuffer = Buffer.allocUnsafe(24);
		offsetNumBuffer.writeUIntLE(offsetNum, 0, 6);
		offsetNum += nodeIndexSize;
		offsetNumBuffer.writeUIntLE(offsetNum, 6, 6);
		offsetNum += wayIndexSize;
		offsetNumBuffer.writeUIntLE(offsetNum, 12, 6);
		offsetNum += relationIndexSize;
		offsetNumBuffer.writeUIntLE(offsetNum, 18, 6);
		indexFileStream.write(offsetNumBuffer);
		console.log("stitching: 1/4 (25%)");
		let tmpStream = fs.createReadStream(path.resolve(tmpDir, "nodes"));
		tmpStream.pipe(indexFileStream, {end: false});
		await new Promise(resolve => tmpStream.on("close", resolve));
		console.log("stitching: 2/4 (50%)");
		tmpStream = fs.createReadStream(path.resolve(tmpDir, "ways"));
		tmpStream.pipe(indexFileStream, {end: false});
		await new Promise(resolve => tmpStream.on("close", resolve));
		console.log("stitching: 3/4 (75%)");
		tmpStream = fs.createReadStream(path.resolve(tmpDir, "relations"));
		tmpStream.pipe(indexFileStream);
		await new Promise(resolve => tmpStream.on("close", resolve));
		console.log("stitching: 4/4 (100%)");
		await mapReader.stop();
	}catch(ex){
		process.exitCode = 1;
		console.error(ex);
	}
})();