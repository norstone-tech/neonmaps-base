const {promises: fsp} = require("fs");
const {NumericIndexFileSearcher} = require("./searcher");
const INT48_SIZE = 6;
const ELEMENT_INDEX_MAGIC_SIZE = 23;
const ELEMENT_INDEX_CHECKSUM_SIZE = 64;
const ELEMENT_INDEX_OFFSETS_START = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE;
const ELEMENT_INDEX_HEADER_SIZE = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE + INT48_SIZE * 4;

class OSMElementIDIndexReader {
	/**
	 * @param {string} mapPath 
	 * @param {Promise<Buffer>} checksum
	 */
	constructor(mapPath, checksum){
		const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
		this.filePath = path.resolve(mapPath, "..", mapName + ".neonmaps.element_index");
		this.checksum = checksum;
	}
	async init(){
		try{
			this.fd = await fsp.open(this.filePath);
			const indexFileHeader = (await this.eifd.read(
				Buffer.allocUnsafe(ELEMENT_INDEX_HEADER_SIZE),
				0,
				ELEMENT_INDEX_HEADER_SIZE,
				0
			)).buffer;
			if(indexFileHeader.slice(0, ELEMENT_INDEX_MAGIC_SIZE).toString("ascii") !== "neonmaps.element_index\0"){
				throw new Error("Element index file is not an element index file");
			}
			if(
				this.checksum && !indexFileHeader.slice(
					ELEMENT_INDEX_MAGIC_SIZE,
					ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE
				).equals(await this.checksum)
			){
				throw new Error("Element index file doesn't match with the map file");
			}
			let fileOffset = ELEMENT_INDEX_OFFSETS_START;
			const nodeIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const wayIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const relationIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const endIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			// Now for some good ol' binary searchers
			this.nodeIndex = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeIndexOffset,
				wayIndexOffset,
				false
			);
			this.wayIndex = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayIndexOffset,
				relationIndexOffset,
				false
			);
			this.relationIndex = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				relationIndexOffset,
				endIndexOffset,
				false
			);
		}finally{
			if(this.fd != null){
				this.fd.close().catch(Function.prototype);
				this.fd = null;
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
module.exports = {OSMElementIDIndexReader}
