const os = require("os");
const path = require("path");
const {MapReaderBase} = require("../lib/map-reader-base");
const {MapReader} = require("../lib/map-reader");
const {FastNodePositionResolver} = require("../lib/indexer/node-positions");
const bounds = require("binary-search-bounds");
const {program} = require('commander');
const {promises: fsp} = require("fs");
const fs = require("fs");
const crypto = require("crypto");
const turf = require("@turf/helpers");
const {default: geoIsClockwise} = require("@turf/boolean-clockwise");
const {default: geoContains} = require("@turf/boolean-contains");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const Pbf = require("pbf");
const {
	WayGeometryBlock: WayGeometryBlockParser,
	RelationGeometryBlock: RelationGeometryBlockParser
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "..", "lib", "proto-defs", "neomaps-cache.proto")))
);
const OSM_NODE = 0;
const OSM_WAY = 1;
const OSM_RELATION = 2;
const MAX_ID_VALUE = 2 ** 48 - 1; // This thing uses unsigned int48s for indexing
const INT32_SIZE = 4;
const INT48_SIZE = 6;
const ELEMENT_INDEX_MAGIC_SIZE = 23;
const ELEMENT_INDEX_CHECKSUM_SIZE = 64;
const ELEMENT_INDEX_OFFSETS_START = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE;

const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option("--no-sanity-check", "Skip file validation")
	.option("--no-elem-index", "Do not create element index")
	.option("--no-parent-index", "Do not create element parent index")
	.option("--no-geometry", "Do not create geometry files")
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
	const {size: mapSize} = await fsp.stat(mapPath);
	const mapHeader = await mapReader.readMapSegment(0);
	let readWay = false;
	let readRelation = false;
	let fileOffset = mapHeader._byte_size;
	let lastNodeId = 0;
	let lastWayId = 0;
	let lastRelationId = 0;
	let segmentCount = 0;
	while(fileOffset < mapSize){
		/**@type {import("../lib/map-reader-base").OSMData} */
		const rawData = await mapReader.readMapSegment(fileOffset);
		const mapSegment = MapReaderBase.decodeRawData(rawData);
		if(mapSegment.nodes.length && (readWay || readRelation)){
			throw new Error("Node appeared _after_ a relation segment or way segment");
		}
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
		readWay = readWay || mapSegment.ways.length > 0;
		if(mapSegment.ways.length && (readRelation)){
			throw new Error("Way appeared _after_ a relation segment");
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
		readRelation = readRelation || mapSegment.relations.length > 0;
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

const cachedWayPointCache = new Map();
/**
 * @typedef InternalPolygon
 * @property {boolean} closed
 * @property {boolean} [inner]
 * @property {Array<number>} lat
 * @property {Array<number>} lon
 */
const getCachedWayPoints = async function(
	/**@type {fsp.FileHandle}*/ fd,
	/**@type {MapReader} */ indexedMapReader,
	/**@type {Map<number, number>} */ offsetMap,
	/**@type {number}*/ wayID
){
	const offsetRefIndex = await indexedMapReader.wayElementFinder.le(wayID)
	if(offsetRefIndex === -1){
		return null;
	}
	const geoFileOffset = offsetMap.get(
		(await indexedMapReader.wayElementFinder.item(offsetRefIndex)).readUIntLE(INT48_SIZE, INT48_SIZE)
	);
	let wayGeometries;
	if(cachedWayPointCache.has(geoFileOffset)){
		wayGeometries = cachedWayPointCache.get(geoFileOffset);
		cachedWayPointCache.delete(geoFileOffset);
		cachedWayPointCache.set(geoFileOffset, wayGeometries);
	}else{
		const geoBlockLength = (
			await fd.read(Buffer.allocUnsafe(INT32_SIZE), 0, INT32_SIZE, geoFileOffset)
		).buffer.readUInt32LE();
		wayGeometries = WayGeometryBlockParser.read(new Pbf(
			(await fd.read(Buffer.allocUnsafe(geoBlockLength), 0, geoBlockLength, geoFileOffset + INT32_SIZE)).buffer
		)).geometries;
		cachedWayPointCache.set(geoFileOffset, wayGeometries);
		while(cachedWayPointCache.size > 50){
			cachedWayPointCache.delete(cachedWayPointCache.keys().next().value);
		}
	}

	const geometry = wayGeometries[bounds.eq(wayGeometries, {id: wayID}, (a, b) => a.id - b.id)];
	if(geometry == null){
		return null;
	}
	/**@type {Array<number>} */
	const lat = [];
	/**@type {Array<number>} */
	const lon = [];
	let curLat = 0;
	let curLon = 0;
	for(let i = 0; i < geometry.geometry.lat.length; i += 1){
		curLat += geometry.geometry.lat[i];
		curLon += geometry.geometry.lon[i];
		lat.push(curLat);
		lon.push(curLon);
	}
	return {
		closed: Boolean(geometry.geometry.closed),
		lat,
		lon
	};
}
const deltaEncodeNums = function(/**@type {Array<number>*/ input){
	/**@type {Array<number>} */
	const result = [];
	let lastNum = 0;
	for(let i = 0; i < input.length; i += 1){
		result.push(input[i] - lastNum);
		lastNum = input[i];
	}
	return result;
}
const matchPointOrderToRingType = function(/**@type {InternalPolygon}*/ poly){
	if(poly.inner == null){
		return;
	}
	const points = poly.lat.map((v, i) => [v, poly.lon[i]]);
	points.push(points[0]);
	// inner rings (holes) should be clockwise
	if(geoIsClockwise(turf.lineString(points)) != poly.inner){
		poly.lat.reverse();
		poly.lon.reverse();
	}
}
const addPointsToIncompletePoly = function(
	/**@type {Set<InternalPolygon>}*/ incompletePolys,
	/**@type {Array<InternalPolygon>}*/ completePolys,
	/**@type {InternalPolygon}*/ pointsToAdd,
	/**@type {boolean}*/ internal
){
	if(pointsToAdd.closed){
		completePolys.push(pointsToAdd);
		return pointsToAdd;
	}
	/**@type {InternalPolygon} */
	let poly;
	for(const p of incompletePolys){
		const lastToAdd = pointsToAdd.lat.length - 1;
		const last = p.lat.length - 1;
		/* https://wiki.openstreetmap.org/wiki/Relation:multipolygon says:
		   The direction of the ways does not matter. The order of the relation members does not matter. ffs */
		if(p.lat[0] == pointsToAdd.lat[0] && p.lon[0] == pointsToAdd.lon[0]){
			const latToAdd = pointsToAdd.lat.reverse();
			latToAdd.pop();
			const lonToAdd = pointsToAdd.lon.reverse();
			lonToAdd.pop();
			p.lat.unshift(...latToAdd);
			p.lon.unshift(...lonToAdd);
		}else if(p.lat[0] == pointsToAdd.lat[lastToAdd] && p.lon[0] == pointsToAdd.lon[lastToAdd]){
			const latToAdd = pointsToAdd.lat;
			latToAdd.pop();
			const lonToAdd = pointsToAdd.lon;
			lonToAdd.pop();
			p.lat.unshift(...latToAdd);
			p.lon.unshift(...lonToAdd);
		}else if(p.lat[last] == pointsToAdd.lat[lastToAdd] && p.lon[last] == pointsToAdd.lon[lastToAdd]){
			const latToAdd = pointsToAdd.lat.reverse();
			latToAdd.shift();
			const lonToAdd = pointsToAdd.lon.reverse();
			lonToAdd.shift();
			p.lat.push(...latToAdd);
			p.lon.push(...lonToAdd);
		}else if(p.lat[last] == pointsToAdd.lat[0] && p.lon[last] == pointsToAdd.lon[0]){
			const latToAdd = pointsToAdd.lat;
			latToAdd.shift();
			const lonToAdd = pointsToAdd.lon;
			lonToAdd.shift();
			p.lat.push(...latToAdd);
			p.lon.push(...lonToAdd);
		}else{
			continue;
		}
		poly = p;
		break;
	}
	if(poly == null){
		if(internal){
			poly = pointsToAdd;
		}else{
			poly = {
				closed: false,
				lat: pointsToAdd.lat,
				lon: pointsToAdd.lon
			};
		}
		incompletePolys.add(poly);
	}else{
		const last = poly.lat.length - 1;
		if(poly.lat[0] == poly.lat[last] && poly.lon[0] == poly.lon[last]){
			poly.lat.pop();
			poly.lon.pop();
			poly.closed = true;
			incompletePolys.delete(poly);
			completePolys.push(poly);
			return poly;
		}else{
			// Check to see if we connect even more lines together
			incompletePolys.delete(poly); // It's gonna probably get re-added anyway
			return addPointsToIncompletePoly(incompletePolys, completePolys, poly, true);
		}
	}
}
// This assumes there are no intersections, no filled polygons overlap, and "outer" vs "inner" polygons are defined correctly
const groupHolesWithPolygons = function(/**@type {Array<turf.Feature<turf.Polygon>>}*/ polys){
	/* First we assign all poly's a "depth" of how "inner" they are, we could probably sort this first to make it more
	   performant, but this is good enough for now... even if it is O(n**2) */
	/**@type {Array<Array<turf.Feature<turf.Polygon>>>} */
	const polygonByDepth = [];
	for(let i = 0; i < polys.length; i += 1){
		let depth = 0;
		for(let ii = 0; ii < polys.length; ii += 1){
			if(ii == i){
				continue;
			}
			if(geoContains(polys[ii], polys[i])){
				depth += 1;
			}
		}
		polys[i].properties.depth = depth;
		if(polygonByDepth[depth] == null){
			polygonByDepth[depth] = [];
		}
		polygonByDepth[depth].push(polys[i]);
	}
	// TODO: Incomplete
}
const getMultipolyGeo = async function(
	/**@type {import("../lib/map-reader-base").OSMRelation}*/ relation,
	/**@type {fsp.FileHandle}*/ fd,
	/**@type {MapReader} */ mapReader,
	/**@type {Map<number, number>} */ offsetMap
) {
	// const subareaMembers = relation.members.filter(mem => mem.role == "subarea");
	const members = relation.members.filter(mem =>
		mem.type == "way" && (
			mem.role == "" || // deprecated alias for "outer"
			mem.role == "outer" ||
			mem.role == "inner"
		)
	);
	/**@type {Set<InternalPolygon>} */
	const incompletePolys = new Set();
	/**@type {Array<InternalPolygon>} */
	const completePolys = [];
	for(let i = 0; i < members.length; i += 1){
		const wayPoints = await getCachedWayPoints(fd, mapReader, offsetMap, members[i].id);
		if(wayPoints == null){
			// TODO: Fall back on subareas if they exist
			console.error(
				"WARNING: Relation " + relation.id + " refers to ways which don't exist; " +
				"Geometry will be omitted!"
			);
			return null;
		}
		const completePoly = addPointsToIncompletePoly(incompletePolys, completePolys, wayPoints);
		if(completePoly != null){
			completePoly.inner = members[i].role == "inner"; // yes, I am just trusting whatever the last way said
			matchPointOrderToRingType(completePoly);
		}
	}
	const completePolyTurf = completePolys.map(poly => {
		const coords = poly.lat.map((lat, i) => [lat, poly.lon[i]]);
		coords.push(coords[0]);
		return turf.polygon([coords], {original: poly, depth: 0});
	});

	// This makes converting to GEOJSON multipolygons easier later on


	for(let i = 0; i < completePolys.length; i += 1){
		completePolyTurf[i].properties.original.inner;
		const poly = completePolyTurf[i].properties.original;
		poly.lat = deltaEncodeNums(poly.lat);
		poly.lon = deltaEncodeNums(poly.lon);
		completePolys[i] = poly;
	}
	if(incompletePolys.size){
		console.error("WARNING: Relation " + relation.id + " contains unclosed polygons; Geometry will be incorrect!");
	}
	if(!completePolys.length){
		console.error("WARNING: Relation " + relation.id + " contains no closed polygons; Geometry will be omitted!");
		return null;
	}
	return completePolys;
};

const geometryMap = async function(mapPath, mapSize, tmpDir, fileOffset, mapFileHashPromise){
	const nodePosResolver = new FastNodePositionResolver(tmpDir, mapReader, 1000000, 10);
	await nodePosResolver.createNodePosFiles();

	let relativeFileOffset = 0;
	const relativeEndOffset = mapSize - fileOffset;
	const cachedMapReader = new MapReader(mapPath, 10, 10);
	await cachedMapReader.init();
	/**@type {Map<number, number?} */
	const wayGeoOffsets = new Map();
	let curWayGeoOffset = 0;
	const wayGeoPath = tmpDir + path.sep + "way_geometries";
	/**@type {Map<number, number?} */
	const relGeoOffsets = new Map();
	let curRelGeoOffset = 0;
	const relGeoPath = tmpDir + path.sep + "relation_geometries";

	const wayGeoFile = await fsp.open(wayGeoPath, "w+");
	const relGeoStream = fs.createWriteStream(relGeoPath);
	while(fileOffset < mapSize){
		const wayGeometries = [];
		const relationGeometries = [];

		// Not using cachedMapReader because I only want that to cache nodes
		const rawData = await mapReader.readMapSegment(fileOffset);
		const mapData = MapReaderBase.decodeRawData(rawData);
		/*
		console.time("Node search start");
		const nodeListList = // await Promise.all(
			mapData.ways.map((way, wayIndex) => 
				Promise.all(way.nodes.map(async nodeID => {
					// await new Promise(resolve => {setTimeout(resolve, wayIndex);});
					const node = await cachedMapReader.getNode(nodeID);
					if(node == null){
						return null;
					}
					return [
						Math.round(node.lat * 1000000000),
						Math.round(node.lon * 1000000000)
					];
				}))
			);
		//);
		console.timeEnd("Node search start");
		*/
		/*
		console.time("Node search start");
		const nodeListList = // await Promise.all(
			mapData.ways.map((way, wayIndex) => 
				Promise.all(
					way.nodes.map(nodeID => nodePosResolver.getPosSync(nodeID) ?? (async () => {
						await new Promise(resolve => setTimeout(resolve, wayIndex));
						return nodePosResolver.getPos(nodeID);
					})())
				)
			);
		//);
		console.timeEnd("Node search start");
		*/
		
		// console.time("Node search");
		// This is some funky shit right here
		/**@type {Array<number>} */
		const nodeIDsInWays = [];
		/**@type {Set<number>} */
		const uniqueNodeIDsInWays = new Set();
		for(let i = 0; i < mapData.ways.length; i += 1){
			const way = mapData.ways[i];
			for(let ii = 0; ii < way.nodes.length; ii += 1){
				uniqueNodeIDsInWays.add(way.nodes[ii]);
			}
		}
		uniqueNodeIDsInWays.forEach(nodeID => {nodeIDsInWays.push(nodeID);})
		nodeIDsInWays.sort((a, b) => a - b);
		uniqueNodeIDsInWays.clear();
		const nodePosInWays = await nodePosResolver.getPos(nodeIDsInWays);
		// console.timeEnd("Node search");
		// console.time("way assembly");
		for(let i = 0; i < mapData.ways.length; i += 1){
			const way = mapData.ways[i];
			const nodesPos = way.nodes.map(nodeID => nodePosInWays[bounds.eq(nodeIDsInWays, nodeID)]);
			
			if(nodesPos.includes(null)){
				console.error(
					"WARNING: Way " + way.id + " refers to nodes which don't exist! " +
					"Geometry will not be included..."
				);
				//continue;
			}
			
			const encodedLat = [];
			const encodedLon = [];
			let lastLat = 0;
			let lastLon = 0;
			const nodesLast = nodesPos.length - 1;
			const nodesLen = way.nodes[0] === way.nodes[nodesLast] ? nodesLast : nodesPos.length;
			for(let ii = 0; ii < nodesLen; ii += 1){
				if(nodesPos[ii] == null){
					throw new Error("Couldn't get pos for " + way.nodes[ii]);
				}
				const lat = nodesPos[ii][0];
				const lon = nodesPos[ii][1];
				encodedLat.push(lat - lastLat);
				encodedLon.push(lon - lastLon);
				lastLat = lat;
				lastLon = lon;
			}
			wayGeometries.push({
				id: way.id,
				geometry: {
					closed: nodesLen == nodesLast,
					lat: encodedLat,
					lon: encodedLon
				}
			});
		}
		// console.timeEnd("way assembly");
		if(wayGeometries.length){
			const pbf = new Pbf();
			WayGeometryBlockParser.write({geometries: wayGeometries}, pbf);
			const pbfBuf = pbf.finish();
			const geoBuf = Buffer.concat([Buffer.allocUnsafe(INT32_SIZE), pbfBuf]);
			wayGeoOffsets.set(fileOffset, curWayGeoOffset);
			geoBuf.writeUInt32LE(pbfBuf.length);
			wayGeoFile.write(geoBuf, 0, geoBuf.length, curWayGeoOffset);
			curWayGeoOffset += geoBuf.length;
		}

		for(let i = 0; i < mapData.relations.length; i += 1){
			const relation = mapData.relations[i];
			const relationType = relation.tags.get("type");
			if(relationType == "multipolygon" || relationType == "boundary"){
				// console.time("Multipolygon resolving");
				const geometry = await getMultipolyGeo(relation, wayGeoFile, cachedMapReader, wayGeoOffsets);
				// console.timeEnd("Multipolygon resolving");
				if(geometry != null){
					relationGeometries.push({
						id: relation.id,
						geometry
					});
				}
			}else if(relationType == "route"){
				const members = relation.members.filter(mem =>
					mem.type == "way" && (
						mem.role == "" ||
						mem.role == "route" || // alias of ""
						mem.role == "forward" ||
						mem.role == "backward" ||
						mem.role == "north" ||
						mem.role == "south" ||
						mem.role == "east" ||
						mem.role == "west" ||
						mem.role == "hail_and_ride" ||
						mem.role == "reverse" ||
						mem.role == "link"
					)
				);
				/**@type {Set<InternalPolygon>} */
				const incompletePolys = new Set();
				/**@type {Array<InternalPolygon>} */
				const geometry = [];
				// console.time("Route resolving");
				for(let i = 0; i < members.length; i += 1){
					const wayPoints = await getCachedWayPoints(
						wayGeoFile,
						cachedMapReader,
						wayGeoOffsets,
						members[i].id
					);
					if(wayPoints == null){
						continue;
					}
					addPointsToIncompletePoly(incompletePolys, geometry, wayPoints);
				}
				// console.timeEnd("Route resolving");
				geometry.push(...incompletePolys);
				for(let i = 0; i < geometry.length; i += 1){
					geometry[i].lat = deltaEncodeNums(geometry[i].lat);
					geometry[i].lon = deltaEncodeNums(geometry[i].lon);
				}
				if(geometry.length){
					relationGeometries.push({
						id: relation.id,
						geometry
					});
				}
			}
		}
		if(relationGeometries.length){
			const pbf = new Pbf();
			RelationGeometryBlockParser.write({geometries: relationGeometries}, pbf);
			const pbfBuf = pbf.finish();
			const geoBuf = Buffer.concat([Buffer.allocUnsafe(INT32_SIZE), pbfBuf]);
			relGeoOffsets.set(fileOffset, curRelGeoOffset);
			geoBuf.writeUInt32LE(pbfBuf.length);
			await writeAndWait(relGeoStream, geoBuf);
			curRelGeoOffset += geoBuf.length;
		}
		fileOffset += rawData._byte_size;
		relativeFileOffset += rawData._byte_size;
		logProgressMsg(
			"Element geometry resolving: " + relativeFileOffset + "/" + relativeEndOffset + " (" +
			(relativeFileOffset / relativeEndOffset * 100).toFixed(2) +
			"%)"
		);
	}
	console.log("Element geometry resolving: " + relativeEndOffset + "/" + relativeEndOffset + " (100%)");
	const closePromise = Promise.all([
		new Promise(resolve => relGeoStream.once("close", resolve)),
		wayGeoFile.close(),
		cachedMapReader.stop()
	]);
	const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
	const geoFileStream = fs.createWriteStream(path.resolve(mapPath, "..", mapName + ".neonmaps.geometry"));
	const fileMagic = "neonmaps.geometry\0";
	geoFileStream.write(fileMagic); // NUL is the version number, which is now 0.
	geoFileStream.write(await mapFileHashPromise);
	const offsetNumBuffer = Buffer.allocUnsafe(INT48_SIZE * 3);
	const wayOffsetStart = fileMagic.length + 64 + offsetNumBuffer.length; // 512 bits -> 64 bytes
	offsetNumBuffer.writeUIntLE(wayOffsetStart, 0, INT48_SIZE);
	const relOffsetStart = wayOffsetStart + wayGeoOffsets.size * 2 * INT48_SIZE;
	offsetNumBuffer.writeUIntLE(relOffsetStart, INT48_SIZE, INT48_SIZE);
	const relOffsetEnd = relOffsetStart + relGeoOffsets.size * 2 * INT48_SIZE;
	offsetNumBuffer.writeUIntLE(relOffsetEnd, INT48_SIZE, INT48_SIZE);

	geoFileStream.write(offsetNumBuffer);
	relGeoStream.end();
	await closePromise;
	let thingsWritten = 0;
	const thingsToWrite = wayGeoOffsets.size + relGeoOffsets.size + 2;
	console.log("Element geometry file writing: 0/" + thingsToWrite + " (0%)");
	for(const [mapOffset, fileOffset] of wayGeoOffsets){
		thingsWritten += 1;
		const offsetBuffer = Buffer.alloc(INT48_SIZE * 2);
		offsetBuffer.writeUIntLE(mapOffset, 0, INT48_SIZE);
		offsetBuffer.writeUIntLE(fileOffset + wayOffsetStart, INT48_SIZE, INT48_SIZE);
		await writeAndWait(geoFileStream, offsetBuffer);
		logProgressMsg(
			"Element geometry file writing: " + thingsWritten + "/" + thingsToWrite + " (" +
			(thingsWritten / thingsToWrite * 100).toFixed(2) +
			"%)"
		);
	}
	for(const [mapOffset, fileOffset] of relGeoOffsets){
		thingsWritten += 1;
		const offsetBuffer = Buffer.alloc(INT48_SIZE * 2);
		offsetBuffer.writeUIntLE(mapOffset, 0, INT48_SIZE);
		offsetBuffer.writeUIntLE(fileOffset + relOffsetStart, INT48_SIZE, INT48_SIZE);
		await writeAndWait(geoFileStream, offsetBuffer);
		logProgressMsg(
			"Element geometry file writing: " + thingsWritten + "/" + thingsToWrite + " (" +
			(thingsWritten / thingsToWrite * 100).toFixed(2) +
			"%)"
		);
	}
	let readStream = fs.createReadStream(wayGeoPath);
	readStream.pipe(geoFileStream, {end: true});
	await new Promise(resolve => readStream.once("close", resolve));
	thingsWritten += 1;
	logProgressMsg(
		"Element geometry file writing: " + thingsWritten + "/" + thingsToWrite + " (" +
		(thingsWritten / thingsToWrite * 100).toFixed(2) +
		"%)"
	);
	readStream = fs.createReadStream(relGeoPath);
	readStream.pipe(geoFileStream);
	await new Promise(resolve => readStream.once("close", resolve));
	console.log("Element geometry file writing: " + thingsToWrite + "/" + thingsToWrite + " (100%)");
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
		if(options.geometry){
			await geometryMap(mapPath, mapSize, tmpDir, firstOffsetWithNonNode, mapFileHashPromise);
		}
		await fsp.rm(tmpDir, {recursive: true, force: true});
		await mapReader.stop();
	}catch(ex){
		process.exitCode = 1;
		console.error(ex);
	}
})();
