const fs = require("fs");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const path = require("path");
const {
	UnpackedElementsCache,
	WayGeometryBlock,
	RelationGeometryBlock
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "neomaps-cache.proto")))
);
const {
	BlobHeader,
	Blob
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "fileformat.proto")))
);
const {
	HeaderBlock,
	PrimitiveBlock
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "osmformat.proto")))
);
module.exports = {
	UnpackedElementsCache,
	WayGeometryBlock,
	RelationGeometryBlock,
	BlobHeader,
	Blob,
	HeaderBlock,
	PrimitiveBlock
};
