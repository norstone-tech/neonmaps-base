const Pbf = require("pbf");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const {MapReader} = require("../map-reader");
const {promises: fsp} = require("fs");
const fs = require("fs");
const path = require("path");
const {CachedNodePositions} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "..", "proto-defs", "neomaps-cache.proto")))
);
let nextProgressMsg = Date.now();
const logProgressMsg = function(...msg){
	if(nextProgressMsg <= Date.now()){
		console.log(...msg);
		nextProgressMsg = Date.now() + 300;
	}
}

class FastNodePositionResolver {
	/**
	 * @param {string} dir 
	 * @param {MapReader} mapReader 
	 * @param {number} [segmentSize=100000]
	 * @param {number} [cacheAmount=50]
	 */
	constructor(dir, mapReader, segmentSize = 100000, cacheAmount = 50){
		this.dir = dir;
		this.mapReader = mapReader;
		this.segmentSize = segmentSize;
		/**@type {Map<number, Map<number, Array<number>> | Promise<Map<number, Array<number>>>} */
		this.positions = new Map();
		this.maxCacheAmount = cacheAmount;
	}
	async createNodePosFiles(){
		let fileOffset = (await this.mapReader.readMapSegment(0))._byte_size;
		const mapSize = (await fsp.stat(this.mapReader.filePath)).size;
		let currentWriteFile = 0;
		let nextWriteFile = this.segmentSize;
		let lastId = 0;
		let lastLat = 0;
		let lastLon = 0;
		let obj = {id: [], lat: [], lon: []};
		while(fileOffset < mapSize){
			/**@type {import('../map-reader-base').OSMData} */
			const rawData = await this.mapReader.readMapSegment(fileOffset);
			if(rawData.primitivegroup.findIndex(group => group.dense != null || group.nodes.length > 0) != -1){
				const {nodes} = MapReader.decodeRawData(rawData);
				for(const node of nodes){
					if(node.id >= 503000000 && node.id < 504000000){
						debugger;
					}
					if(node.id >= nextWriteFile){
						const pbf = new Pbf();
						CachedNodePositions.write(obj, pbf);
						const pbfBuf = pbf.finish();
						await fsp.writeFile(this.dir + path.sep + "nodepos_" + currentWriteFile, pbfBuf);
						// console.log("wrote " + currentWriteFile + "; size " + pbfBuf.length + " with " + obj.id.length + " items");
						while(node.id >= nextWriteFile){
							currentWriteFile += this.segmentSize;
							nextWriteFile += this.segmentSize;
						}
						lastId = 0;
						lastLat = 0;
						lastLon = 0;
						obj = {id: [], lat: [], lon: []};
					}
					obj.id.push(node.id - lastId);
					lastId = node.id;
					const lat = Math.round(node.lat * 1000000000);
					obj.lat.push(lat - lastLat);
					lastLat = lat;
					const lon = Math.round(node.lon * 1000000000);
					obj.lon.push(lon - lastLon);
					lastLon = lon;
				}
			}
			fileOffset += rawData._byte_size;
			logProgressMsg(
				"Node position indexing: " + fileOffset + "/" + mapSize + " (" +
				(fileOffset / mapSize * 100).toFixed(2) +
				"%)"
			);
		}
		const pbf = new Pbf();
		CachedNodePositions.write(obj, pbf);
		await fsp.writeFile(this.dir + path.sep + "nodepos_" + currentWriteFile, pbf.finish());
		console.log("Node position indexing: " + mapSize + "/" + mapSize + " (100%)");
	}
	async getPos(nodeID){
		const fileID = nodeID - (nodeID % this.segmentSize);
		if(this.positions.has(fileID)){
			const result = this.positions.get(fileID);
			this.positions.delete(fileID);
			this.positions.set(fileID, result);
			return (await result).get(nodeID);
		}
		const result = (async () => {
			// console.time("read " + fileID);
			let lastId = 0;
			let lastLat = 0;
			let lastLon = 0;
			/**@type {Map<number, Array<number>>} */
			const posMap = new Map();
			const obj = CachedNodePositions.read(new Pbf(await fsp.readFile(this.dir + path.sep + "nodepos_" + fileID)));
			for(let i = 0; i < obj.id.length; i += 1){
				lastId += obj.id[i];
				lastLat += obj.lat[i];
				lastLon += obj.lon[i];
				posMap.set(lastId, [lastLat, lastLon]);
			}
			this.positions.set(fileID, posMap);
			// console.timeEnd("read " + fileID);
			return posMap;
		})();
		this.positions.set(fileID, result);
		while(this.positions.size > this.maxCacheAmount){
			this.positions.delete(this.positions.keys().next().value);
		}
		return (await result).get(nodeID);
	}
	getPosSync(nodeID){
		const fileID = nodeID - (nodeID % this.segmentSize);
		const map = this.positions.get(fileID);
		if(map == null || map instanceof Promise){
			return;
		}
		this.positions.delete(fileID);
		this.positions.set(fileID, map);
		return map.get(nodeID);
	}
}
module.exports = {FastNodePositionResolver};
