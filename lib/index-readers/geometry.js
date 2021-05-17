const {promises: fsp} = require("fs");
const path = require("path");
const bounds = require('binary-search-bounds');
const Pbf = require("pbf");
const {NumericIndexFileSearcher} = require("./searcher");
const {OSMElementIDIndexReader} = require("./elem-id");
const INT32_SIZE = 4;
const INT48_SIZE = 6;
const GEO_FILE_MAGIC = Buffer.from("neonmaps.geometry\0");
const GEO_FILE_MAGIC_SIZE = GEO_FILE_MAGIC.length;
const GEO_FILE_CHECKSUM_SIZE = 64;
const GEO_FILE_OFFSETS_START = GEO_FILE_MAGIC_SIZE + GEO_FILE_CHECKSUM_SIZE;
const GEO_FILE_HEADER_SIZE = GEO_FILE_OFFSETS_START + INT48_SIZE * 3;
const {WayGeometryBlock: WayGeometryBlockParser, RelationGeometryBlock: RelationGeometryBlockParser} = require("../proto-defs");
const symbolDeltaDecoded = Symbol("deltaDecoded");

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
	 * @param {OSMElementIDIndexReader} elemIndexReader
	 * @param {number} wayCacheAmount
	 * @param {number} relationCacheAmount
	 * @param {Promise<Buffer>} [mapChecksum]
	 */
	constructor(mapPath, elemIndexReader, wayCacheAmount, relationCacheAmount, mapChecksum){
		const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
		this.filePath = path.resolve(mapPath, "..", mapName + ".neonmaps.geometry");
		this.wayMapOffsetFinder = elemIndexReader.wayIndex;
		this.relMapOffsetFinder = elemIndexReader.relationIndex;
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
			const relOffsetMapStart = fileHeader.readUIntLE(GEO_FILE_OFFSETS_START + INT48_SIZE, INT48_SIZE);
			const relOffsetMapEnd = fileHeader.readUIntLE(GEO_FILE_OFFSETS_START + INT48_SIZE * 2, INT48_SIZE);
			this.wayGeoOffsetFinder = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayOffsetMapStart,
				relOffsetMapStart
			);
			this.relGeoOffsetFinder = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				relOffsetMapStart,
				relOffsetMapEnd
			);
			await Promise.all([
				this.wayGeoOffsetFinder.init(),
				this.relGeoOffsetFinder.init()
			]);
		}catch(ex){
			if(this.fd != null){
				this.fd.close().catch(Function.prototype);
				this.fd = null;
			}
			throw ex;
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
				/**@type {Array<InternalWayGeometry>} */
				const geoArray = WayGeometryBlockParser.read(new Pbf(
					(
						await this.fd.read(Buffer.allocUnsafe(geoBufferSize), 0, geoBufferSize, wayGeoOffset + INT32_SIZE)
					).buffer
				)).geometries;
				if(this.wayCache.has(wayMapOffset)){
					this.wayCache.set(wayMapOffset, geoArray);
				}
				return geoArray;
			})();
			this.wayCache.set(wayMapOffset, geoArrayPromise);
			while(this.wayCache.size > this.maxWayCacheAmount){
				this.wayCache.delete(this.wayCache.keys().next().value);
			}
			geoArray = await geoArrayPromise;
			if(this.wayCache.has(wayMapOffset)){
				this.wayCache.set(wayMapOffset, geoArray);
			}
		}
		const result = geoArray[bounds.eq(geoArray, {id: wayID}, (a, b) => (a.id - b.id))];
		if(result != null && !result[symbolDeltaDecoded]){
			const {geometry} = result;
			let lastLat = 0;
			let lastLon = 0;
			for(let i = 0; i < geometry.lat.length; i += 1){
				geometry.lat[i] = (lastLat += geometry.lat[i]);
				geometry.lon[i] = (lastLon += geometry.lon[i]);
			}
			result[symbolDeltaDecoded] = true;
		}
		return result;
	}
	async getRelationGeometry(relID){
		const relMapOffsetIndex = await this.relMapOffsetFinder.le(relID);
		if(relMapOffsetIndex == -1){
			return null;
		}
		const relMapOffset = (
			await this.relMapOffsetFinder.item(relMapOffsetIndex)
		).readUIntLE(INT48_SIZE, INT48_SIZE);
		/**@type {Array<InternalRelationGeometry>} */
		let geoArray;
		if(this.relCache.has(relMapOffset)){
			geoArray = await this.relCache.get(relMapOffset);
		}else{
			const geoArrayPromise = (async () => {
				const relGeoOffsetIndex = await this.relGeoOffsetFinder.eq(relMapOffset);
				if(relMapOffsetIndex == -1){
					// This may happen if all relations in this block have invalid geometry
					return [];
				}
				const relGeoOffset = (
					await this.relGeoOffsetFinder.item(relGeoOffsetIndex)
				).readUIntLE(INT48_SIZE, INT48_SIZE);
				const geoBufferSize = (
					await this.fd.read(Buffer.allocUnsafe(INT32_SIZE), 0, INT32_SIZE, relGeoOffset)
				).buffer.readUInt32LE();
				/**@type {Array<InternalWayGeometry>} */
				const geoArray = RelationGeometryBlockParser.read(new Pbf(
					(
						await this.fd.read(Buffer.allocUnsafe(geoBufferSize), 0, geoBufferSize, relGeoOffset + INT32_SIZE)
					).buffer
				)).geometries;
				if(this.relCache.has(relMapOffset)){
					this.relCache.set(relMapOffset, geoArray);
				}
				return geoArray;
			})();
			this.relCache.set(relMapOffset, geoArrayPromise);
			while(this.relCache.size > this.maxRelCacheAmount){
				this.relCache.delete(this.relCache.keys().next().value);
			}
			geoArray = await geoArrayPromise;
			if(this.relCache.has(relMapOffset)){
				this.relCache.set(relMapOffset, geoArray);
			}
		}
		const result = geoArray[bounds.eq(geoArray, {id: relID}, (a, b) => (a.id - b.id))];
		if(result != null && !result[symbolDeltaDecoded]){
			const {geometry} = result;
			for(let i = 0; i < geometry.length; i += 1){
				let lastLat = 0;
				let lastLon = 0;
				for(let ii = 0; ii < geometry[i].lat.length; ii += 1){
					geometry[i].lat[ii] = (lastLat += geometry[i].lat[ii]);
					geometry[i].lon[ii] = (lastLon += geometry[i].lon[ii]);
				}
			}
			result[symbolDeltaDecoded] = true;
		}
		return result;
	}
	async stop(){
		if(this.fd != null){
			await this.fd.close();
			this.fd = null;
		}
	}
}
module.exports = {MapGeometryReader};
