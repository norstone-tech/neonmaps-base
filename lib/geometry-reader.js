const {promises: fsp} = require("fs");
const path = require("path");
const bounds = require('binary-search-bounds');
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const Pbf = require("pbf");
const {NumericIndexFileSearcher} = require("./index-file-searcher");
const INT32_SIZE = 4;
const INT48_SIZE = 6;
const GEO_FILE_MAGIC = Buffer.from("neonmaps.geometry\0");
const GEO_FILE_MAGIC_SIZE = GEO_FILE_MAGIC.length;
const GEO_FILE_CHECKSUM_SIZE = 64;
const GEO_FILE_OFFSETS_START = GEO_FILE_MAGIC_SIZE + GEO_FILE_CHECKSUM_SIZE;
const GEO_FILE_HEADER_SIZE = GEO_FILE_OFFSETS_START + INT48_SIZE * 3;
const {
	WayGeometryBlock: WayGeometryBlockParser,
	RelationGeometryBlock: RelationGeometryBlockParser
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "proto-defs", "neomaps-cache.proto")))
);
/**
 * @typedef InternalGeometryPoints
 * @property {boolean} closed
 * @property {boolean} [inner]
 * @property {Array<number>} lat
 * @property {Array<number>} lon
 */
/**
 * @typedef InternalWayGeometry
 * @property {number} id
 * @property {InternalGeometryPoints} geometry
 */
/**
 * @typedef InternalRelationGeometry
 * @property {number} id
 * @property {Array<InternalGeometryPoints>} geometry
 */
class MapGeometryReader {
	/**
	 * @param {string} mapPath 
	 * @param {NumericIndexFileSearcher} wayOffsetFinder
	 * @param {NumericIndexFileSearcher} relationOffsetFinder
	 * @param {number} wayCacheAmount
	 * @param {number} relationCacheAmount
	 * @param {Promise<Buffer>} [mapChecksum]
	 */
	constructor(mapPath, wayOffsetFinder, relationOffsetFinder, wayCacheAmount, relationCacheAmount, mapChecksum){
		const mapName = filePath.substring(filePath.lastIndexOf(path.sep) + 1, filePath.length - ".osm.pbf".length);
		this.filePath = path.resolve(mapPath, "..", mapName + ".neonmaps.geometry");
		this.wayMapOffsetFinder = wayOffsetFinder;
		this.relationMapOffsetFinder = relationOffsetFinder;
		/**@type {Map<number, Promise<Array<InternalWayGeometry>> | Array<InternalWayGeometry>>} */
		this.wayCache = new Map();
		this.maxWayCacheAmount = wayCacheAmount;
		/**@type {Map<number, Promise<Array<InternalRelationGeometry>> | Array<InternalRelationGeometry>>} */
		this.relCache = new Map();
		this.maxRelCacheAmount = relationCacheAmount;
		this.mapChecksum = mapChecksum;
	}
	async init(){
		try{
			this.fd = await fsp.open(this.filePath);
			const fileHeader = (await this.fd.read(
				Buffer.allocUnsafe(GEO_FILE_HEADER_SIZE),
				0,
				GEO_FILE_HEADER_SIZE,
				0
			)).buffer;
			if(!fileHeader.slice(0, GEO_FILE_MAGIC_SIZE).equals(GEO_FILE_MAGIC)){
				throw new Error("Geometry file is not a geometry file");
			}
			if(
				this.mapChecksum != null &&
				!fileHeader.slice(GEO_FILE_MAGIC_SIZE, GEO_FILE_MAGIC_SIZE + GEO_FILE_CHECKSUM_SIZE).equals(
					await this.mapChecksum
				)
			){
				throw new Error("Geometry index file doesn't match with the map file");
			}
			const wayOffsetMapStart = fileHeader.readUIntLE(GEO_FILE_OFFSETS_START, INT48_SIZE);
			const nodeOffsetMapStart = fileHeader.readUIntLE(GEO_FILE_OFFSETS_START + INT48_SIZE, INT48_SIZE);
			const nodeOffsetMapEnd = fileHeader.readUIntLE(GEO_FILE_OFFSETS_START + INT48_SIZE * 2, INT48_SIZE);
			this.wayGeoOffsetFinder = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayOffsetMapStart,
				nodeOffsetMapStart
			);
			this.nodeGeoOffsetFinder = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeOffsetMapStart,
				nodeOffsetMapEnd
			);
		}finally{
			if(this.fd != null){
				this.fd.close().catch(Function.prototype);
				this.fd = null;
			}
		}
	}
	async getWayGeometry(wayID){
		const wayMapOffsetIndex = await this.wayMapOffsetFinder.le(wayID);
		if(wayMapOffsetIndex == -1){
			return null;
		}
		const wayMapOffset = (
			await this.wayMapOffsetFinder.item(wayMapOffsetIndex)
		).readUIntLE(INT48_SIZE, INT48_SIZE);
		/**@type {Array<InternalWayGeometry>} */
		let geoArray;
		if(this.wayCache.has(wayMapOffset)){
			geoArray = await this.wayCache.get(wayMapOffset);
		}else{
			const geoArrayPromise = (async () => {
				const wayGeoOffsetIndex = await this.wayGeoOffsetFinder.eq(wayMapOffset);
				if(wayMapOffsetIndex == -1){
					// This should never happen, all ways have geometries
					return [];
				}
				const wayGeoOffset = (
					await this.wayGeoOffsetFinder.item(wayGeoOffsetIndex)
				).readUIntLE(INT48_SIZE, INT48_SIZE);
				const geoBufferSize = (
					await this.fd.read(Buffer.allocUnsafe(INT32_SIZE), 0, INT32_SIZE, wayGeoOffset)
				).buffer.readUInt32LE();
				const geoArray = WayGeometryBlockParser.read(new Pbf(
					await this.fd.read(Buffer.allocUnsafe(geoBufferSize), 0, geoBufferSize, wayGeoOffset + INT32_SIZE)
				)).geometries;
			})();
			this.wayCache.set(wayMapOffset, geoArrayPromise);
			geoArray = await geoArrayPromise;
			if(this.wayCache.has(wayMapOffset)){
				this.wayCache.set(wayMapOffset, geoArray);
			}
		}
	}
	async stop(){
		if(this.fd != null){
			await this.fd.close();
			this.fd = null;
		}
	}
}
module.exports = {MapGeometryReader};