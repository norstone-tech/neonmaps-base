const INT48_SIZE = 6;
const PARENT_INDEX_MAGIC_SIZE = 22;
const PARENT_INDEX_CHECKSUM_SIZE = 64;
const PARENT_INDEX_OFFSETS_START = PARENT_INDEX_MAGIC_SIZE + PARENT_INDEX_CHECKSUM_SIZE;
const PARENT_INDEX_HEADER_SIZE = PARENT_INDEX_MAGIC_SIZE + PARENT_INDEX_CHECKSUM_SIZE + 1 + INT48_SIZE * 4;
const PARENT_INDEX_LIST_ITEM_COUNT = 5;
// const PARENT_INDEX_LIST_ITEM_SIZE = PARENT_INDEX_LIST_ITEM_COUNT * INT48_SIZE;
const {NumericIndexFileSearcher} = require("./searcher");
class OSMElementParentIndexReader {
	/**
	 * @param {string} mapPath 
	 * @param {Promise<Buffer>} checksum
	 */
	constructor(mapPath, checksum){
		const mapName = mapPath.substring(mapPath.lastIndexOf(path.sep) + 1, mapPath.length - ".osm.pbf".length);
		this.filePath = path.resolve(mapPath, "..", mapName + ".neonmaps.parent_index");
		this.checksum = checksum;
	}
	async init(){
		try{
			this.fd = await fsp.open(this.filePath);
			this.fd = await fsp.open(this.parentIndexFilePath);
			const parentFileHeader = (await this.fd.read(
				Buffer.allocUnsafe(PARENT_INDEX_HEADER_SIZE),
				0,
				PARENT_INDEX_HEADER_SIZE,
				0
			)).buffer;
			if(parentFileHeader.slice(0, PARENT_INDEX_MAGIC_SIZE).toString("ascii") !== "neonmaps.parent_index\0"){
				throw new Error("Element index file is not an element index file");
			}
			if(
				this.checksum && !parentFileHeader.slice(
					PARENT_INDEX_MAGIC_SIZE,
					PARENT_INDEX_MAGIC_SIZE + PARENT_INDEX_CHECKSUM_SIZE
				).equals(await this.checksum)
			){
				throw new Error("Parent index file doesn't match with the map file");
			}
			fileOffset = PARENT_INDEX_OFFSETS_START;
			this._parentArrayLengthByteLength = parentFileHeader[fileOffset];
			fileOffset += 1;
			// The parent index file has these encoded at sizes, not offsets
			const nodeToWayOffset = PARENT_INDEX_HEADER_SIZE; 
			const nodeToRelOffset = nodeToWayOffset +
			parentFileHeader.readUIntLE(fileOffset, INT48_SIZE) * INT48_SIZE * 2;
				fileOffset += INT48_SIZE;
			const wayToRelOffset = nodeToRelOffset +
				parentFileHeader.readUIntLE(fileOffset, INT48_SIZE) * INT48_SIZE * 2;
			fileOffset += INT48_SIZE;
			const relToRelOffset = wayToRelOffset +
				parentFileHeader.readUIntLE(fileOffset, INT48_SIZE) * INT48_SIZE * 2;
			fileOffset += INT48_SIZE;
			const parentEndOffset = relToRelOffset +
				parentFileHeader.readUIntLE(fileOffset, INT48_SIZE) * INT48_SIZE * 2;
			
			this.nodeToWay = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeToWayOffset,
				nodeToRelOffset,
				false
			);
			this.nodeToRel = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeToRelOffset,
				wayToRelOffset,
				false
			);
			this.wayToRel = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayToRelOffset,
				relToRelOffset,
				false
			);
			this.relToRel = new NumericIndexFileSearcher(
				this.fd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				relToRelOffset,
				parentEndOffset,
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
module.exports = {OSMElementParentIndexReader};