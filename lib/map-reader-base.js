const {promises: fsp} = require("fs");
const fs = require("fs");
const path = require("path");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const Pbf = require("pbf");
const zlib = require("zlib");
class MapWithToJSON extends Map{
	toJSON(){
		const result = {};
		this.forEach((val, key) => {
			result[key] = val;
		});
		return result;
	}
}
const freezeRecrusive = function(o){
	for(const k in o){
		const v = o[k];
		if(typeof v === "object" && v !== null){
			freezeRecrusive(v);
		}
	}
	Object.freeze(o);
}
const {
	BlobHeader: MapBlobHeaderParser,
	Blob: MapBlobParser
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "proto-defs", "fileformat.proto")))
);
const {
	HeaderBlock: OSMHeaderParser,
	PrimitiveBlock: OSMDataParser
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "proto-defs", "osmformat.proto")))
);
const OSM_MAP_HEADER_SIZE_LENGTH = 4;
const OSM_MAP_KNOWN_FEATURES = new Set(["OsmSchema-V0.6", "DenseNodes"]);

/**
 * @typedef InternalMapBlobHeader
 * (Internal use only)
 * @property {number} datasize
 * @property {Uint8Array | null} indexdata
 * @property {string} type
 * @private
 */
/**
 * @typedef InternalMapBlob
 * (Internal use only)
 * @property {number | null} raw_size
 * @property {"raw" | "zlib_data" | "lzma_data" | "OBSOLETE_bzip2_data" | "lz4_data" | "zstd_data"} data
 * @property {Buffer | null} raw
 * @property {Buffer | null} zlib_data
 * @property {Buffer | null} lzma_data
 * @property {Buffer | null} OBSOLETE_bzip2_data
 * @property {Buffer | null} lz4_data
 * @property {Buffer | null} zstd_data
 */
/**
 * @typedef OSMHeaderBBox
 * @property {number} left
 * @property {number} right
 * @property {number} top
 * @property {number} bottom
 */
/**
 * @typedef OSMHeader
 * @property {number} _byte_size
 * @property {OSMHeaderBBox | null} bbox
 * @property {Array<string>} required_features
 * @property {Array<string>} optional_features
 * @property {string | null} writingprogram
 * @property {string | null} source
 * @property {number | null} osmosis_replication_timestamp
 * @property {number | null} osmosis_replication_sequence_number
 * @property {string | null} osmosis_replication_base_url
 */

/**
 * @typedef OSMStringTable
 * @property {Array<Buffer>} s
 */
/**
 * @typedef OSMRawInfo
 * @property {number | null} version
 * @property {number | null} timestamp
 * @property {number | null} changeset
 * @property {number | null} uid
 * @property {number | null} user_sid
 */
/**
 * @typedef OSMRawNode
 * @property {number} id
 * @property {Array<number>} keys
 * @property {Array<number>} vals
 * @property {OSMRawInfo | null} info
 * @property {number} lat
 * @property {number} lon
 */
/**
 * @typedef OSMRawDenseInfo
 * @property {Array<number>} version
 * @property {Array<number>} timestamp
 * @property {Array<number>} changeset
 * @property {Array<number>} uid
 * @property {Array<number>} user_sid
 */
/**
 * @typedef OSMRawDenseNodes
 * @property {Array<number>} id
 * @property {OSMRawDenseInfo | null} denseinfo
 * @property {Array<number>} lat
 * @property {Array<number>} lon
 * @property {Array<number>} keys_vals
 */
/**
 * @typedef OSMRawWay
 * @property {number} id
 * @property {Array<number>} keys
 * @property {Array<number>} vals
 * @property {OSMRawInfo | null} info
 * @property {Array<number>} refs
 * @property {Array<number> | null} lat - Only available if LocationsOnWays is one of the optional_features
 * @property {Array<number> | null} lon - Only available if LocationsOnWays is one of the optional_features
 */
/**
 * @typedef OSMRawRelation
 * @property {number} id
 * @property {Array<number>} keys
 * @property {Array<number>} vals
 * @property {OSMRawInfo | null} info
 * @property {Array<number>} roles_sid
 * @property {Array<number>} memids
 * @property {Array<0 | 1 | 2>} types
 */
/**
 * @typedef OSMPrimitiveGroup
 * @property {Array<OSMRawNode>} nodes
 * @property {OSMRawDenseNodes | null} dense
 * @property {Array<OSMRawWay>} ways
 * @property {Array<OSMRawRelation>} relations
 * @property {Array<Object>} changesets
 */
/**
 * @typedef OSMData
 * @property {number} _byte_size
 * @property {OSMStringTable} stringtable
 * @property {Array<OSMPrimitiveGroup>} primitivegroup
 * @property {number} granularity
 * @property {number} lat_offset
 * @property {number} lon_offset
 * @property {number} date_granularity
 */
/**
 * @typedef OSMNode
 * @property {"node"} type
 * @property {number} id
 * @property {Map<string, string>} tags
 * @property {number} [version]
 * @property {Date} [timestamp]
 * @property {number} [changeset]
 * @property {number} [uid]
 * @property {string} [user]
 * @property {number} lat
 * @property {number} lon
 */
/**
 * @typedef OSMWay
 * @property {"way"} type
 * @property {number} id
 * @property {Map<string, string>} tags
 * @property {number} [version]
 * @property {Date} [timestamp]
 * @property {number} [changeset]
 * @property {number} [uid]
 * @property {string} [user]
 * @property {Array<number>} nodes
 */
/**
 * @typedef OSMRelationMember
 * @property {number} id
 * @property {"node" | "way" | "relation"} type
 * @property {string} role
 */
/**
 * @typedef OSMRelation
 * @property {"relation"} type
 * @property {number} id
 * @property {Map<string, string>} tags
 * @property {number} [version]
 * @property {Date} [timestamp]
 * @property {number} [changeset]
 * @property {number} [uid]
 * @property {string} [user]
 * @property {Array<OSMRelationMember>} members
 */
/**
 * @typedef OSMDecodeResult
 * @property {Array<OSMNode>} nodes
 * @property {Array<OSMWay>} ways
 * @property {Array<OSMRelation>} relations
 */
const relationMemberTypeEnums = ["node", "way", "relation"];
class MapReaderBase {
	/**
	 * Reads .osm.pbf files 
	 * @param {string} filePath 
	 * @param {number} [cacheAmount=0]
	 */
	constructor(filePath, cacheAmount = 0){
		this.filePath = path.resolve(filePath);
		this.maxCacheAmount = cacheAmount;
		/**@ */
		this.cache = new Map();
	}
	async init(){
		try{
			this.fd = await fsp.open(this.filePath); // Read only by default
			/**@type {OSMHeader} */
			const rawHeaderData = await this.readMapSegment(0);
			for(let i = 0; i < rawHeaderData.required_features.length; i += 1){
				const feat = rawHeaderData.required_features[i];
				if(!OSM_MAP_KNOWN_FEATURES.has(feat)){
					throw new Error("This map file contains unknown feature " + feat);
				}
			}
		}catch(ex){
			if(this.fd != null){
				await this.stop();
			}
			throw ex;
		}
	}
	async stop(){
		if(this.fd != null){
			await this.fd.close();
			this.fd = null;
		}
	}
	/**
	 * @async
	 * Reads a chunk of the map file
	 * @param {number} size 
	 * @param {number} offset 
	 * @returns {Promise<Buffer>}
	 */
	async readFileChunk(size, offset){
		return (await this.fd.read(Buffer.allocUnsafe(size), 0, size, offset)).buffer;
	}
	/**
	 * @async
	 * @param {number} [offset=0] 
	 * @returns {Promise<OSMData | OSMHeader>}
	 */
	async readMapSegment(offset = 0){
		let firstOffset = offset;
		const headerSize = (await this.readFileChunk(OSM_MAP_HEADER_SIZE_LENGTH, offset)).readUInt32BE(0);
		offset += OSM_MAP_HEADER_SIZE_LENGTH;
		/**@type {InternalMapBlobHeader} */
		const mapBlobHeader = MapBlobHeaderParser.read(new Pbf(await this.readFileChunk(headerSize, offset)));
		offset += headerSize;
		/**@type {InternalMapBlob} */
		const mapBlob = await MapBlobParser.read(new Pbf(await this.readFileChunk(mapBlobHeader.datasize, offset)));
		offset += mapBlobHeader.datasize;
		let mapBlobBuffer = mapBlob[mapBlob.data];
		switch(mapBlob.data){
			case "OBSOLETE_bzip2_data":
				throw new Error("bzip2 compressed data not supported");
			case "lz4_data":
				// https://www.npmjs.com/package/lz4-asm
				throw new Error("lz4 compressed data not supported");
			case "lzma_data":
				throw new Error("lzma compressed data not supported");
			case "zlib_data":
				mapBlobBuffer = await new Promise((resolve, reject) => {
					zlib.unzip(mapBlobBuffer, (err, result) => {
						if(err){
							reject(err);
						}else{
							resolve(result);
						}
					});
				});
				break;
			case "zstd_data":
				throw new Error("zstd compressed data not supported");
			case "raw":
			default:
				// Do nothing
		}
		if(mapBlob.raw_size != null && mapBlob.raw_size !== mapBlobBuffer.length){
			throw new Error(
				"Expected decompressed map data to have size " + mapBlob.raw_size + " got " +
				mapBlobBuffer.length
			);
		}
		/**@type {OSMData | OSMHeader} */
		let result;
		switch(mapBlobHeader.type){
			case "OSMHeader":
				result = OSMHeaderParser.read(new Pbf(mapBlobBuffer));
				break;
			case "OSMData":
				result = OSMDataParser.read(new Pbf(mapBlobBuffer));
				break;
			default:
				throw new Error("Unknown blob type: " + mapBlobHeader.type);
		}
		result._byte_size = offset - firstOffset;
		// TODO: freezeRecrusive if cache is enabled
		return result;
	}
}
/**
 * @param {OSMData} rawData 
 * @returns {OSMDecodeResult}
 */
MapReaderBase.decodeRawData = function(rawData){
	/**@type {Array<OSMNode>} */
	const nodes = [];
	/**@type {Array<OSMWay>} */
	const ways = [];
	/**@type {Array<OSMRelation>} */
	const relations = [];
	const {
		date_granularity: dateGranularity,
		granularity: posGranularity,
		lat_offset: latOffset,
		lon_offset: lonOffset,
		stringtable: {
			s: stringtable
		}
	} = rawData;
	for(let i = 0; i < rawData.primitivegroup.length; i += 1){
		const rawGroup = rawData.primitivegroup[i];
		if(rawGroup.dense){
			const denseNodes = rawGroup.dense;
			let lastRawLat = denseNodes.lat[0];
			let lastRawLon = denseNodes.lon[0];
			let lastRawUserSid = 0;
			/**@type {OSMNode} */
			let lastUnpackedNode = {
				type: "node",
				id: denseNodes.id[0],
				lat: 0.000000001 * (latOffset + (posGranularity * lastRawLat)),
				lon: 0.000000001 * (lonOffset + (posGranularity * lastRawLon)),
				tags: new MapWithToJSON()
			};
			if(denseNodes.denseinfo){
				const {denseinfo} = denseNodes;
				lastUnpackedNode.changeset = denseinfo.changeset[0];
				lastUnpackedNode.timestamp = new Date(denseinfo.timestamp[0] * dateGranularity);
				lastUnpackedNode.uid = denseinfo.uid[0];
				lastRawUserSid = denseinfo.user_sid[0];
				lastUnpackedNode.user = stringtable[lastRawUserSid].toString();
				lastUnpackedNode.version = denseinfo.version[0];
			}
			let iii = 0;
			// this can be an empty array, luckily undefined is also falsey
			while(denseNodes.keys_vals[iii]){
				lastUnpackedNode.tags.set(
					stringtable[denseNodes.keys_vals[iii]].toString(),
					stringtable[denseNodes.keys_vals[iii + 1]].toString()
				);
				iii += 2;
			}
			nodes.push(lastUnpackedNode);
			iii += 1;
			for(let ii = 1; ii < denseNodes.id.length; ii += 1){
				lastRawLat += denseNodes.lat[ii];
				lastRawLon += denseNodes.lon[ii];
				/**@type {OSMNode} */
				const unpackedNode = {
					type: "node",
					id: lastUnpackedNode.id + denseNodes.id[ii],
					lon: 0.000000001 * (lonOffset + (posGranularity * lastRawLon)),
					lat: 0.000000001 * (latOffset + (posGranularity * lastRawLat)),
					tags: new MapWithToJSON()
				};
				if(denseNodes.denseinfo){
					const {denseinfo} = denseNodes;
					unpackedNode.changeset = lastUnpackedNode.changeset + denseinfo.changeset[ii];
					unpackedNode.timestamp = new Date(
						lastUnpackedNode.timestamp.getTime() +
						denseinfo.timestamp[ii] * dateGranularity
					);
					unpackedNode.uid = lastUnpackedNode.uid + denseinfo.uid[ii];
					lastRawUserSid += denseinfo.user_sid[ii];
					unpackedNode.user = stringtable[lastRawUserSid].toString();
					unpackedNode.version = denseinfo.version[ii];
				}
				while(denseNodes.keys_vals[iii]){
					unpackedNode.tags.set(
						stringtable[denseNodes.keys_vals[iii]].toString(),
						stringtable[denseNodes.keys_vals[iii + 1]].toString()
					);
					iii += 2;
				}
				nodes.push(unpackedNode);
				iii += 1;
				lastUnpackedNode = unpackedNode;
			}
		}
		for(let ii = 0; ii < rawGroup.nodes.length; ii += 1){
			const rawNode = rawGroup.nodes[ii];
			/**@type {OSMNode} */
			const node = {
				id: rawNode.id,
				type: "node",
				lat: 0.000000001 * (latOffset + (posGranularity * rawNode.lat)),
				lon: 0.000000001 * (lonOffset + (posGranularity * rawNode.lon)),
				tags: new MapWithToJSON()
			};
			for(let iii = 0; iii < rawNode.keys.length; iii += 1){
				node.tags.set(
					stringtable[rawNode.keys[iii]].toString(),
					stringtable[rawNode.vals[iii]].toString()
				);
			}
			if(rawNode.info){
				node.changeset = rawNode.info.changeset;
				node.timestamp = new Date(rawNode.info.timestamp * dateGranularity);
				node.uid = rawNode.info.uid;
				node.user = stringtable[rawNode.info.user_sid].toString();
				node.version = rawNode.info.version;
			}
			nodes.push(node);
		}
		for(let ii = 0; ii < rawGroup.ways.length; ii += 1){
			const rawWay = rawGroup.ways[ii];
			let lastWayRef = 0; // I'm counting on Array.prototype.map being sequential for this to work
			/**@type {OSMWay} */
			const way = {
				type: "way",
				id: rawWay.id,
				nodes: rawWay.refs.map(deltaVal => lastWayRef += deltaVal),
				tags: new MapWithToJSON()
			};
			for(let iii = 0; iii < rawWay.keys.length; iii += 1){
				way.tags.set(
					stringtable[rawWay.keys[iii]].toString(),
					stringtable[rawWay.vals[iii]].toString()
				);
			}
			if(rawWay.info){
				way.changeset = rawWay.info.changeset;
				way.timestamp = new Date(rawWay.info.timestamp * dateGranularity);
				way.uid = rawWay.info.uid;
				way.user = stringtable[rawWay.info.user_sid].toString();
				way.version = rawWay.info.version;
			}
			ways.push(way);
		}
		for(let ii = 0; ii < rawGroup.relations.length; ii += 1){
			let lastMemId = 0;
			const rawRelation = rawGroup.relations[ii];
			/**@type {OSMRelation} */
			const relation = {
				type: "relation",
				id: rawRelation.id,
				members: rawRelation.memids.map((memid, iii) => {
					return {
						id: lastMemId += memid,
						role: stringtable[rawRelation.roles_sid[iii]].toString(),
						type: relationMemberTypeEnums[rawRelation.types[iii]]
					}
				}),
				tags: new MapWithToJSON()
			};
			for(let iii = 0; iii < rawRelation.keys.length; iii += 1){
				relation.tags.set(
					stringtable[rawRelation.keys[iii]].toString(),
					stringtable[rawRelation.vals[iii]].toString()
				);
			}
			if(rawRelation.info){
				relation.changeset = rawRelation.info.changeset;
				relation.timestamp = new Date(rawRelation.info.timestamp * dateGranularity);
				relation.uid = rawRelation.info.uid;
				relation.user = stringtable[rawRelation.info.user_sid].toString();
				relation.version = rawRelation.info.version;
			}
			relations.push(relation);
		}
	}
	return {nodes, ways, relations};
}
module.exports = {MapReaderBase};