const {promises: fsp} = require("fs");
const path = require("path");
const bounds = require('binary-search-bounds');
const {MapReaderBase} = require("./map-reader-base");
const {NumericIndexFileSearcher} = require("./index-file-searcher");
const ELEMENT_INDEX_MAGIC_SIZE = 23;
const ELEMENT_INDEX_CHECKSUM_SIZE = 64;
const ELEMENT_INDEX_OFFSETS_START = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE;
const ELEMENT_INDEX_OFFSETS_SIZE = 24;
const ELEMENT_INDEX_OFFSET_SIZE = 6;
const ELEMENT_INDEX_HEADER_SIZE = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE + ELEMENT_INDEX_OFFSETS_SIZE;

class MapReader extends MapReaderBase {
	/**
	 * @param {string} filePath
	 * @param {number} [rawCacheAmount=0]
	 * @param {number} [decodedCacheAmount=0]
	 * @param {boolean} [checksum=false]
	 */
	constructor(filePath, rawCacheAmount = 0, decodedCacheAmount = 0, checksum){
		super(filePath, rawCacheAmount, decodedCacheAmount, checksum);
		const mapName = filePath.substring(filePath.lastIndexOf(path.sep) + 1, filePath.length - ".osm.pbf".length);
		this.elemIndexFilePath = path.resolve(filePath, "..", mapName + ".neonmaps.element_index");
	}
	async init(){
		await super.init();
		try{
			this.eifd = await fsp.open(this.elemIndexFilePath);
			const indexFileHeader = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_HEADER_SIZE),
				0,
				ELEMENT_INDEX_HEADER_SIZE,
				0
			)).buffer;
			if(indexFileHeader.slice(0, ELEMENT_INDEX_MAGIC_SIZE).toString("ascii") !== "neonmaps.element_index\0"){
				throw new Error("Element index file is not an element index file");
			}
			if(
				this.checksum && !indexFileHeader.slice(
					ELEMENT_INDEX_MAGIC_SIZE,
					ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE
				).equals(await this.checksum)
			){
				throw new Error("Element index file doesn't match with the map file");
			}
			// This next bit is fugly
			let fileOffset = ELEMENT_INDEX_OFFSETS_START;
			const nodeIndexOffset = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_OFFSET_SIZE),
				0,
				ELEMENT_INDEX_OFFSET_SIZE,
				fileOffset
			)).buffer.readUIntLE(0, ELEMENT_INDEX_OFFSET_SIZE);
			fileOffset += ELEMENT_INDEX_OFFSET_SIZE;
			const wayIndexOffset = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_OFFSET_SIZE),
				0,
				ELEMENT_INDEX_OFFSET_SIZE,
				fileOffset
			)).buffer.readUIntLE(0, ELEMENT_INDEX_OFFSET_SIZE);
			fileOffset += ELEMENT_INDEX_OFFSET_SIZE;
			const relationIndexOffset = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_OFFSET_SIZE),
				0,
				ELEMENT_INDEX_OFFSET_SIZE,
				fileOffset
			)).buffer.readUIntLE(0, ELEMENT_INDEX_OFFSET_SIZE);
			fileOffset += ELEMENT_INDEX_OFFSET_SIZE;
			const endIndexOffset = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_OFFSET_SIZE),
				0,
				ELEMENT_INDEX_OFFSET_SIZE,
				fileOffset
			)).buffer.readUIntLE(0, ELEMENT_INDEX_OFFSET_SIZE);
			// Now for some good ol' binary searchers
			this.nodeElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				ELEMENT_INDEX_OFFSET_SIZE * 2,
				ELEMENT_INDEX_OFFSET_SIZE,
				0,
				nodeIndexOffset,
				wayIndexOffset,
				false
			);
			this.wayElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				ELEMENT_INDEX_OFFSET_SIZE * 2,
				ELEMENT_INDEX_OFFSET_SIZE,
				0,
				wayIndexOffset,
				relationIndexOffset,
				false
			);
			this.relationElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				ELEMENT_INDEX_OFFSET_SIZE * 2,
				ELEMENT_INDEX_OFFSET_SIZE,
				0,
				relationIndexOffset,
				endIndexOffset,
				false
			);
			// No cleanup is required on error since we passed our fd to these
			await Promise.all([
				this.nodeElementFinder.init(),
				this.wayElementFinder.init(),
				this.relationElementFinder.init()
			]);
		}catch(ex){
			if(this.eifd != null){
				await this.eifd.close();
			}
			await super.stop();
			throw ex;
		}
	}
	async getNode(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.nodeElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.nodeElementFinder.item(offsetRefIndex)).readUIntLE(
			ELEMENT_INDEX_OFFSET_SIZE,
			ELEMENT_INDEX_OFFSET_SIZE
		);
		const {nodes} = await this.readDecodedMapSegment(mapFileOffset);
		const nodeIndex = bounds.eq(nodes, {id}, (a, b) => a.id - b.id);
		if(nodeIndex === -1){
			return null;
		}
		return nodes[nodeIndex];
	}
	async getWay(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		console.time("find map offset");
		const offsetRefIndex = await this.wayElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.wayElementFinder.item(offsetRefIndex)).readUIntLE(
			ELEMENT_INDEX_OFFSET_SIZE,
			ELEMENT_INDEX_OFFSET_SIZE
		);
		console.timeEnd("find map offset");
		//const {ways} = await this.readDecodedMapSegment(mapFileOffset);
		console.time("read map data");
		const rawData = await this.readMapSegment(mapFileOffset);
		console.timeEnd("read map data");
		console.time("decode map data");
		const {ways} = MapReader.decodeRawData(rawData);
		console.timeEnd("decode map data");
		console.time("find way");
		const wayIndex = bounds.eq(ways, {id}, (a, b) => a.id - b.id);
		console.timeEnd("find way");
		if(wayIndex === -1){
			return null;
		}
		return ways[wayIndex];
	}
	async getRelation(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.relationElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.relationElementFinder.item(offsetRefIndex)).readUIntLE(
			ELEMENT_INDEX_OFFSET_SIZE,
			ELEMENT_INDEX_OFFSET_SIZE
		);
		const {relations} = await this.readDecodedMapSegment(mapFileOffset);
		const relationIndex = bounds.eq(relations, {id}, (a, b) => a.id - b.id);
		if(relationIndex === -1){
			return null;
		}
		return relations[relationIndex];
	}
}

module.exports = {MapReader};