const {promises: fsp} = require("fs");
const bounds = require("binary-search-bounds");
const {NumericIndexFileSearcher} = require("../index-readers/searcher");
const {WayGeometryBlock: WayGeometryBlockParser} = require("../proto-defs");
const Pbf = require("pbf");
const INT32_SIZE = 4;
const INT48_SIZE = 6;
class TempWayGeoFinder {
	/**
	 * @param {fsp.FileHandle} fd 
	 * @param {NumericIndexFileSearcher} wayOffsetFinder 
	 * @param {Map<number, number>} offsetMap 
	 */
	constructor(fd, wayOffsetFinder, offsetMap){
		this.fd = fd;
		this.wayOffsetFinder = wayOffsetFinder;
		this.offsetMap = offsetMap;
	}
	/**
	 * @param {Array<number>} wayIDs sorted 
	 */
	async prepareWayGeometries(wayIDs){
		let wayIndex = 0;
		let wayMapOffsetIndex = await this.wayOffsetFinder.le(wayIDs[0]);
		this._wayIDs = wayIDs;
		if(wayMapOffsetIndex == null){
			this._geometries = (new Array(wayIDs.length)).fill(null);
		}
		/**@type {Array<import("../index-readers/geometry").InternalGeometryPoints>} */
		this._geometries = [];
		while(this._geometries.length < this._wayIDs.length){
			if(wayMapOffsetIndex >= this.wayOffsetFinder.length){
				this._geometries.push(null);
				continue;
			}
			const geoFileOffset = this.offsetMap.get(
				(await this.wayOffsetFinder.item(wayMapOffsetIndex)).readUIntLE(INT48_SIZE, INT48_SIZE)
			);
			const geoBlockLength = (
				await this.fd.read(Buffer.allocUnsafe(INT32_SIZE), 0, INT32_SIZE, geoFileOffset)
			).buffer.readUInt32LE();
			/**@type {Array<import("../index-readers/geometry").InternalWayGeometry>} */
			const wayGeometries = WayGeometryBlockParser.read(new Pbf(
				(await this.fd.read(
						Buffer.allocUnsafe(geoBlockLength),
						0,
						geoBlockLength,
						geoFileOffset + INT32_SIZE
					)
				).buffer
			)).geometries;
			for(let i = 0; i < wayGeometries.length; i += 1){
				const wayGeometry = wayGeometries[i];
				while(wayGeometry.id > wayIDs[wayIndex]){
					this._geometries.push(null);
					wayIndex += 1;
				}
				if(wayGeometry.id == wayIDs[wayIndex]){
					// convert from delta encoding
					const {lat, lon} = wayGeometry.geometry;
					let lastLat = 0;
					let lastLon = 0;
					for(let i = 0; i < lat.length; i += 1){
						lastLat += lat[i];
						lastLon += lon[i];
						lat[i] = lastLat;
						lon[i] = lastLon;
					}
					this._geometries.push(wayGeometry.geometry);
					wayIndex += 1;
				}
			}
			if(wayMapOffsetIndex == (this.wayOffsetFinder.length - 1)){
				wayMapOffsetIndex += 1;
			}else{
				wayMapOffsetIndex = await this.wayOffsetFinder.le(wayIDs[wayIndex], wayMapOffsetIndex);
			}
		}
	}
	getGeometry(wayID){
		const index = bounds.eq(this._wayIDs, wayID);
		return this._geometries[index];
	}
}
module.exports = {TempWayGeoFinder};
