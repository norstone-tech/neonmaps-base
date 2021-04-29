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
	 */
	constructor(dir, mapReader, segmentSize){
		this.dir = dir;
		this.mapReader = mapReader;
		this.segmentSize = segmentSize;
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
	/**
	 * 
	 * @param {Array<number>} nodeIDs must be sorted
	 * @returns {Promise<Array<Array<number>>>}
	 */
	async getPos(nodeIDs){
		const result = [];
		const lastNodeIndex = nodeIDs.length - 1;
		let curNodeIndex = 0;
		const firstFileID = nodeIDs[0] - (nodeIDs[0] % this.segmentSize);
		const lastFileID = nodeIDs[lastNodeIndex] - (nodeIDs[lastNodeIndex] % this.segmentSize);
		for(let fileID = firstFileID; fileID <= lastFileID; fileID += this.segmentSize){
			if(nodeIDs[curNodeIndex] > (fileID + this.segmentSize)){
				continue;
			}
			let lastId = 0;
			let lastLat = 0;
			let lastLon = 0;
			const obj = CachedNodePositions.read(new Pbf(await fsp.readFile(this.dir + path.sep + "nodepos_" + fileID)));
			for(let i = 0; i < obj.id.length; i += 1){
				lastId += obj.id[i];
				lastLat += obj.lat[i];
				lastLon += obj.lon[i];
				while(nodeIDs[curNodeIndex] < lastId){
					result[curNodeIndex] = null;
					curNodeIndex += 1;
				}
				if(nodeIDs[curNodeIndex] == lastId){
					result[curNodeIndex] = [
						lastLat,
						lastLon
					];
					curNodeIndex += 1;
				}
			}
			if(curNodeIndex >= nodeIDs.length){
				break;
			}
		}
		return result;
	}
}
module.exports = {FastNodePositionResolver};
