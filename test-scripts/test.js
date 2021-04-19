

// const fileProto = schema.parse(fs.readFileSync("proto-defs/fileformat.proto"));
// const osmProto = schema.parse(fs.readFileSync("proto-defs/osmformat.proto"));
const {MapReader} = require("../");
const {writeFileSync} = require("fs");
(async () => {
	try{
		const mapReader = new MapReader("ontario.osm.pbf");
		await mapReader.init();
		const header = await mapReader.readMapSegment();
		console.log(header);
		const firstMapSegMent = await mapReader.readMapSegment(header._byte_size);
		console.log(firstMapSegMent._byte_size);
		writeFileSync("test.json", JSON.stringify(MapReader.decodeRawData(firstMapSegMent), null, "\t"))
		await mapReader.stop();
	}catch(ex){
		console.error(ex);
	}
})();
