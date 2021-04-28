const os = require("os");
const path = require("path");
const {MapReaderBase} = require("../lib/map-reader-base");
const {program} = require('commander');
const {promises: fsp} = require("fs");
const fs = require("fs");
const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.parse()
	.opts();

const mapPath = path.resolve(options.map);
const mapReader = new MapReaderBase(mapPath);
let nextProgressMsg = 0;
const logProgressMsg = function(...msg){
	if(nextProgressMsg <= Date.now()){
		console.log(...msg);
		nextProgressMsg = Date.now() + 300;
	}
};
(async() => {
	try{
		const {size: mapSize} = await fsp.stat(mapPath);
		await mapReader.init();
		let fileOffset = (await mapReader.readMapSegment(0))._byte_size;
		let nodeCount = 0;
		let wayCount = 0;
		let relationCount = 0;
		while(fileOffset < mapSize){
			/**@type {import("../lib/map-reader-base").OSMData} */
			const rawData = await mapReader.readMapSegment(fileOffset);
			for(let i = 0; i < rawData.primitivegroup.length; i += 1){
				const rawGroup = rawData.primitivegroup[i];
				nodeCount += rawGroup.nodes.length;
				if(rawGroup.dense){
					nodeCount += rawGroup.dense.id.length;
				}
				wayCount += rawGroup.ways.length;
				relationCount += rawGroup.relations.length;
			}
			fileOffset += rawData._byte_size;
			logProgressMsg(
				"Counting map elements: " + fileOffset + "/" + mapSize + " (" +
				(fileOffset / mapSize * 100).toFixed(2) +
				"%)"
			);
		}
		console.log({
			nodeCount,
			wayCount,
			relationCount
		});
		await mapReader.stop();
	}catch(ex){
		process.exitCode = 1;
		console.error(ex);
	}
})();
