const {promises: fsp} = require("fs");
const fs = require("fs");
const path = require("path");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const Pbf = require("pbf");
const zlib = require("zlib");
const bounds = require('binary-search-bounds');
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
		if(typeof v === "object" && v !== null && !v instanceof ArrayBuffer){
			freezeRecrusive(v);
		}
	}
	Object.freeze(o);
}
const {
	BlobHeader: MapBlobHeaderParser,
	Blob: MapBlobParser,
	HeaderBlock: OSMHeaderParser,
	PrimitiveBlock: OSMDataParser,
	UnpackedElementsCache: UnpackedElementsCacheParser
} = require("./proto-defs");
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
/**
 * @typedef OSMCachedNode
 * @property {number} id
 * @property {Array<string>} tag_keys
 * @property {Array<string>} tag_vals
 * @property {number | null} version
 * @property {number | null} timestamp
 * @property {number | null} changeset
 * @property {number | null} uid
 * @property {string | null} user
 * @property {number} lat
 * @property {number} lon
 */
/**
 * @typedef OSMCachedWay
 * @property {number} id
 * @property {Array<string>} tag_keys
 * @property {Array<string>} tag_vals
 * @property {number | null} version
 * @property {number | null} timestamp
 * @property {number | null} changeset
 * @property {number | null} uid
 * @property {string | null} user
 * @property {Array<number>} nodes
 */
/**
 * @typedef OSMCachedRelation
 * @property {number} id
 * @property {Array<string>} tag_keys
 * @property {Array<string>} tag_vals
 * @property {number | null} version
 * @property {number | null} timestamp
 * @property {number | null} changeset
 * @property {number | null} uid
 * @property {string | null} user
 * @property {Array<OSMRelationMember>} members
 */
/**
 * @typedef OSMCachedResult
 * @property {Array<OSMCachedNode>} nodes
 * @property {Array<OSMCachedWay>} ways
 * @property {Array<OSMCachedRelation>} relations
 */
const relationMemberTypeEnums = ["node", "way", "relation"];
class MapReaderBase {
	/**
	 * Reads .osm.pbf files 
	 * @param {string} filePath
	 * @param {number} [rawCacheAmount=0]
	 * @param {number} [decodedCacheAmount=0]
	 * @param {number} [diskCacheSize=0]
	 * @param {boolean} [checksum=false]
	 */
	 constructor(filePath, rawCacheAmount = 0, decodedCacheAmount = 0, diskCacheSize = 0, checksum){
		this.filePath = path.resolve(filePath);
		this.maxRawCacheAmount = rawCacheAmount;
		this.maxDecodedCacheAmount = decodedCacheAmount;
		this.maxDiskCacheSize = diskCacheSize;
		/**@type {Map<number, OSMData | Promise<OSMData>>} */
		this.rawDataCache = new Map();
		/**@type {Map<number, OSMDecodeResult | Promise<OSMDecodeResult>} */
		this.decodedDataCache = new Map();
		if(checksum){
			/**@type {Promise<Buffer> | null} */
			this.checksum = new Promise((resolve, reject) => {
				const hasher = crypto.createHash("sha512");
				const mapFileStream = fs.createReadStream(this.filePath);
				// TODO: Does this cause memory leaks?
				mapFileStream.once("error", reject);
				mapFileStream.on("data", c => {hasher.update(c);});
				mapFileStream.once("end", () => resolve(hasher.digest()));
			});
		}
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
			freezeRecrusive(rawHeaderData);
			this.mapHeader = rawHeaderData;
			if(this.maxDiskCacheSize > 0){
				const mapName = this.filePath.substring(
					this.filePath.lastIndexOf(path.sep) + 1, this.filePath.length - ".osm.pbf".length
				);
				this.diskCachePath = path.resolve(this.filePath, "..", mapName + ".neonmaps.element_cache");
				// Recursive option ignores if the dir already exists
				await fsp.mkdir(this.diskCachePath, {recursive: true});
				const cacheFiles = await fsp.readdir(this.diskCachePath);
				for(let i = 0; i < cacheFiles.length; i += 1){
					const file = cacheFiles[i];
					// Unexpected shutdown?
					if(file.endsWith("_temp.pbf")){
						/* Force is here because there might be multiple processes pulling from the same cache, and
						   whoever deletes leftover temp files first are anyones guess */
						await fsp.rm(path.resolve(this.diskCachePath, file), {force: true});
					}
					this._clearCacheTimeout = setTimeout(this.cleanDiskCache.bind(this), 2 * 60 * 1000);
				}
				this.diskCachePath += path.sep;
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
		if(this._clearCacheTimeout != null){
			clearTimeout(this._clearCacheTimeout);
			delete this._clearCacheTimeout;
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
		if(this.mapHeader && offset === 0){
			return this.mapHeader;
		}
		if(offset && this.rawDataCache.has(offset)){
			const result = this.rawDataCache.get(offset);
			this.rawDataCache.delete(offset); // Ensure this result is at the "end" so it won't be removed later
			this.rawDataCache.set(offset, result);
			return result;
		}
		let resultReject;
		let resultResolve;
		let resultPromise = new Promise((resolve, reject) => {
			resultResolve = resolve;
			resultReject = reject;
		});
		let firstOffset = offset;
		if(this.maxRawCacheAmount > 0 && firstOffset){
			// This ensures we don't read the same map chunk multiple times
			this.rawDataCache.set(offset, resultPromise);
		}
		try{
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
			if(this.maxRawCacheAmount > 0 && firstOffset){
				freezeRecrusive(result);
				this.rawDataCache.set(firstOffset, result);
				while(this.rawDataCache.size > this.maxRawCacheAmount){
					// Delete the first (and oldest) thing in the set
					this.rawDataCache.delete(this.rawDataCache.keys().next().value);
				}
			}
			resultResolve(result);
			return result;
		}catch(ex){
			resultReject(result);
			this.rawDataCache.delete(firstOffset);
			throw ex;
		}
	}
	/**
	 * @private
	 * @param {number} offset 
	 * @param {OSMDecodeResult} osmData 
	 */
	async _saveToDiskCache(offset, osmData){
		const pbf = new Pbf();
		UnpackedElementsCacheParser.write(MapReaderBase.decodeResultToCache(osmData), pbf);
		// Another process may read the file while I'm writing it maybe
		const tmpFile = this.diskCachePath + Date.now().toString(36) + Math.random().toString(36) + "_temp.pbf";
		await fsp.writeFile(tmpFile, pbf.finish());
		await fsp.rename(tmpFile, this.diskCachePath + offset + ".pbf");
	}
	/**
	 * @param {number} offset 
	 * @returns {OSMDecodeResult}
	 */
	async readDecodedMapSegment(offset){
		if(!offset){
			throw new Error("Offset must not be 0");
		}
		/**@type {OSMDecodeResult} */
		let result;
		if(this.decodedDataCache.has(offset)){
			result = this.decodedDataCache.get(offset);
			this.decodedDataCache.delete(offset); // Ensure this result is at the "end" so it won't be removed later
			this.decodedDataCache.set(offset, result);
			return result;
		}
		let resultReject;
		let resultResolve;
		let resultPromise = new Promise((resolve, reject) => {
			resultResolve = resolve;
			resultReject = reject;
		});
		if(this.maxDecodedCacheAmount > 0){
			// This ensures we don't read the same map chunk multiple times
			this.decodedDataCache.set(offset, resultPromise);
		}
		try{
			if(this.maxDiskCacheSize > 0){
				try{
					result = MapReaderBase.cacheToDecodeResult(
						UnpackedElementsCacheParser.read(
							new Pbf(await fsp.readFile(this.diskCachePath + offset + ".pbf"))
						)
					);
					resultResolve(result);
				}catch(ex){
					if(ex.code != "ENOENT"){
						throw ex;
					}
				}
			}
			if(result == null){
				result = MapReaderBase.decodeRawData(await this.readMapSegment(offset));
				resultResolve(result);
				if(this.maxDiskCacheSize > 0){
					this._saveToDiskCache(offset, result).catch((err) => {
						console.error("Neonmaps: Unable to save to disk cache:", err.name + ": " + err.message);
					});
				}
			}
			if(this.maxDecodedCacheAmount > 0){
				freezeRecrusive(result);
				this.decodedDataCache.set(offset, result);
				while(this.decodedDataCache.size > this.maxDecodedCacheAmount){
					// Delete the first (and oldest) thing in the set
					this.decodedDataCache.delete(this.decodedDataCache.keys().next().value);
				}
			}
			// TODO: freezeRecrusive if cache is enabled
			return result;
		}catch(ex){
			this.decodedDataCache.delete(offset);
			resultReject(ex);
			throw ex;
		}
	}
	async cleanDiskCache(force){
		/* I'm doing it this way because there might be multiple processes pulling from the same disk cache, and we
		   don't know how recently _they_ accessed a file without some sort of inter-process communcation. That might
		   cool, but it's not practical right now */
		if(!this._newDiskCacheCreated && !force){
			return;
		}
		this._newDiskCacheCreated = false;
		const files = (await fsp.readdir(this.diskCachePath)).filter(v => {
			const fileName = v.substring(0, v.length - 4);
			return v.endsWith(".pbf") &&
				!v.endsWith("_temp.pbf") &&
				Number(fileName) === parseInt(fileName); // NaN is also checked here
		});
		let totalSize = 0;
		/**@type {Array<fs.Stats>} */
		const stats = [];
		// Intentionally doing a for loop here because I want this to be slow.
		for(let i = 0; i < files.length; i += 1){
			stats[i] = await fsp.stat(this.diskCachePath + files[i]);
			totalSize += stats[i].size;
		}
		stats.sort((a, b) => a.atimeMs - b.atimeMs);
		for(let i = 0; i < files.length; i += 1){
			if(totalSize <= this.maxDiskCacheSize){
				break;
			}
			await fsp.rm(this.diskCachePath + files[i], {force: true});
			totalSize -= stats[i].size;
		}
		if(this._clearCacheTimeout != null){
			this._clearCacheTimeout.refresh();
		}
	}
}
const baseConvertFromCache = function(/**@type {OSMCachedNode | OSMCachedWay | OSMCachedRelation} */ cacheElem){
	/**@type {OSMNode | OSMWay | OSMRelation} */
	const newElem = cacheElem;
	newElem.tags = new MapWithToJSON();
	for(let i = 0; i < cacheElem.tag_keys.length; i += 1){
		newElem.tags.set(cacheElem.tag_keys[i], cacheElem.tag_vals[i]);
	}
	delete cacheElem.tag_keys;
	delete cacheElem.tag_vals;
	if(cacheElem.changeset == null){
		delete newElem.changeset;
	}
	if(cacheElem.timestamp == null){
		delete newElem.timestamp;
	}else{
		newElem.timestamp = new Date(cacheElem.timestamp);
	}
	if(cacheElem.uid == null){
		delete newElem.uid;
	}
	if(cacheElem.user == null){
		delete newElem.user;
	}
	if(cacheElem.version == null){
		delete newElem.version;
	}
	return newElem;
};

/**
 * Converts the _existing_ object
 * @param {OSMCachedResult} cacheData 
 * @returns {OSMDecodeResult}
 */
MapReaderBase.cacheToDecodeResult = function(cacheData){
	for(let i = 0; i < cacheData.nodes.length; i += 1){
		baseConvertFromCache(cacheData.nodes[i]).type = "node";
	}
	for(let i = 0; i < cacheData.ways.length; i += 1){
		baseConvertFromCache(cacheData.ways[i]).type = "way";
	}
	for(let i = 0; i < cacheData.relations.length; i += 1){
		baseConvertFromCache(cacheData.relations[i]).type = "relation";
	}
	return cacheData;
};
const baseConvertToCache = function(/**@type {OSMNode | OSMWay | OSMRelation} */ osmElem){
	/**@type {OSMCachedNode | OSMCachedWay | OSMCachedRelation} */
	const cachedElem = {
		id: osmElem.id,
		tag_keys: [],
		tag_vals: []
	};
	osmElem.tags.forEach((val, key) => {
		cachedElem.tag_keys.push(key);
		cachedElem.tag_vals.push(val);
	});
	if(osmElem.changeset != null){
		cachedElem.changeset = osmElem.changeset;
	}
	if(osmElem.timestamp != null){
		cachedElem.timestamp = osmElem.timestamp.getTime()
	}
	if(osmElem.uid != null){
		cachedElem.uid = osmElem.uid;
	}
	if(osmElem.user != null){
		cachedElem.user = osmElem.user;
	}
	if(osmElem.version != null){
		cachedElem.version = osmElem.version;
	}
	return cachedElem;
};

/**
 * Creates a _new_ object from the OSM data
 * @param {OSMDecodeResult} osmData 
 * @returns {OSMCachedResult}
 */
MapReaderBase.decodeResultToCache = function(osmData){
	return {
		nodes: osmData.nodes.map((v) => {
			const cachedNode = baseConvertToCache(v);
			cachedNode.lat = v.lat;
			cachedNode.lon = v.lon;
			return cachedNode;
		}),
		ways: osmData.ways.map((v) => {
			const cachedWay = baseConvertToCache(v);
			cachedWay.nodes = v.nodes.slice();
			return cachedWay;
		}),
		relations: osmData.relations.map((v) => {
			const cachedRelation = baseConvertToCache(v);
			cachedRelation.members = v.members.map((v) => {
				return {...v};
			});
			return cachedRelation;
		})
	};
};
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
				if(rawNode.info.changeset != null){
					node.changeset = rawNode.info.changeset;
				}
				if(rawNode.info.timestamp != null){
					node.timestamp = new Date(rawNode.info.timestamp * dateGranularity);
				}
				if(rawNode.info.uid != null){
					node.uid = rawNode.info.uid;
				}
				if(rawNode.info.user_sid != null){
					node.user = stringtable[rawNode.info.user_sid].toString();
				}
				if(rawNode.info.version != null){
					node.version = rawNode.info.version;
				}
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
				if(rawWay.info.changeset != null){
					way.changeset = rawWay.info.changeset;
				}
				if(rawWay.info.timestamp != null){
					way.timestamp = new Date(rawWay.info.timestamp * dateGranularity);
				}
				if(rawWay.info.uid != null){
					way.uid = rawWay.info.uid;
				}
				if(rawWay.info.user_sid != null){
					way.user = stringtable[rawWay.info.user_sid].toString();
				}
				if(rawWay.info.version != null){
					way.version = rawWay.info.version;
				}
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
				if(rawRelation.info.changeset != null){
					relation.changeset = rawRelation.info.changeset;
				}
				if(rawRelation.info.timestamp != null){
					relation.timestamp = new Date(rawRelation.info.timestamp * dateGranularity);
				}
				if(rawRelation.info.uid != null){
					relation.uid = rawRelation.info.uid;
				}
				if(rawRelation.info.user_sid != null){
					relation.user = stringtable[rawRelation.info.user_sid].toString();
				}
				if(rawRelation.info.version != null){
					relation.version = rawRelation.info.version;
				}
			}
			relations.push(relation);
		}
	}
	return {nodes, ways, relations};
};
/**
 * Converts a OSM-JSON element object to the objects this library normally returns
 * Does not create a new object
 * @param {Object} o
 * @returns {OSMNode | OSMWay | OSMRelation}
 */
MapReaderBase.OSMJSONElemToOSMElem = function(o){
	if(o.timestamp != null){
		o.timestamp = new Date();
	}
	const {tags} = o;
	const newTags = new MapWithToJSON();
	if(tags != null){
		for(const k in tags){
			newTags.set(k, tags[k]);
		}
	}
	o.tags = newTags;
	return o;
};

module.exports = {MapReaderBase};
