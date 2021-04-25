const {promises: fsp} = require("fs");
const path = require("path");
const bounds = require('binary-search-bounds');
const {MapReaderBase} = require("./map-reader-base");
const {NumericIndexFileSearcher} = require("./index-file-searcher");

const INT48_SIZE = 6;
const ELEMENT_INDEX_MAGIC_SIZE = 23;
const ELEMENT_INDEX_CHECKSUM_SIZE = 64;
const ELEMENT_INDEX_OFFSETS_START = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE;
const ELEMENT_INDEX_HEADER_SIZE = ELEMENT_INDEX_MAGIC_SIZE + ELEMENT_INDEX_CHECKSUM_SIZE + INT48_SIZE * 4;

const PARENT_INDEX_MAGIC_SIZE = 22;
const PARENT_INDEX_CHECKSUM_SIZE = ELEMENT_INDEX_CHECKSUM_SIZE;
const PARENT_INDEX_OFFSETS_START = PARENT_INDEX_MAGIC_SIZE + PARENT_INDEX_CHECKSUM_SIZE;
const PARENT_INDEX_HEADER_SIZE = PARENT_INDEX_MAGIC_SIZE + PARENT_INDEX_CHECKSUM_SIZE + 1 + INT48_SIZE * 4;
const PARENT_INDEX_LIST_ITEM_COUNT = 5;
const PARENT_INDEX_LIST_ITEM_SIZE = PARENT_INDEX_LIST_ITEM_COUNT * INT48_SIZE;

class MapReader extends MapReaderBase {
	/**
	 * @param {string} filePath
	 * @param {number} [rawCacheAmount=0]
	 * @param {number} [decodedCacheAmount=0]
	 * @param {number} [diskCacheSize=0]
	 * @param {boolean} [checksum=false]
	 */
	constructor(filePath, rawCacheAmount = 0, decodedCacheAmount = 0, diskCacheSize = 0, checksum){
		super(filePath, rawCacheAmount, decodedCacheAmount, diskCacheSize, checksum);
		const mapName = filePath.substring(filePath.lastIndexOf(path.sep) + 1, filePath.length - ".osm.pbf".length);
		this.elemIndexFilePath = path.resolve(filePath, "..", mapName + ".neonmaps.element_index");
		this.parentIndexFilePath = path.resolve(filePath, "..", mapName + ".neonmaps.parent_index");
	}
	async init(){
		await super.init();
		try{
			// Init element indexer
			this.eifd = await fsp.open(this.elemIndexFilePath);
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
			// This next bit is fugly
			let fileOffset = ELEMENT_INDEX_OFFSETS_START;
			const nodeIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const wayIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const relationIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			fileOffset += INT48_SIZE;
			const endIndexOffset = indexFileHeader.readUIntLE(fileOffset, INT48_SIZE);
			// Now for some good ol' binary searchers
			this.nodeElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeIndexOffset,
				wayIndexOffset,
				false
			);
			this.wayElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayIndexOffset,
				relationIndexOffset,
				false
			);
			this.relationElementFinder = new NumericIndexFileSearcher(
				this.eifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				relationIndexOffset,
				endIndexOffset,
				false
			);
			// Init parent indexer, should this be optional?
			this.pifd = await fsp.open(this.parentIndexFilePath);
			const parentFileHeader = (await this.pifd.read(
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
			
			this.nodeToWayFinder = new NumericIndexFileSearcher(
				this.pifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeToWayOffset,
				nodeToRelOffset,
				false
			);
			this.nodeToRelFinder = new NumericIndexFileSearcher(
				this.pifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				nodeToRelOffset,
				wayToRelOffset,
				false
			);
			this.wayToRelFinder = new NumericIndexFileSearcher(
				this.pifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				wayToRelOffset,
				relToRelOffset,
				false
			);
			this.relToRelFinder = new NumericIndexFileSearcher(
				this.pifd,
				INT48_SIZE * 2,
				INT48_SIZE,
				0,
				relToRelOffset,
				parentEndOffset,
				false
			);
			// No cleanup is required on error since we passed our fd to these
			await Promise.all([
				this.nodeElementFinder.init(),
				this.wayElementFinder.init(),
				this.relationElementFinder.init(),
				this.nodeToWayFinder.init(),
				this.nodeToRelFinder.init(),
				this.wayToRelFinder.init(),
				this.relToRelFinder.init()
			]);
		}catch(ex){
			if(this.eifd != null){
				await this.eifd.close();
			}
			if(this.pifd != null){
				await this.pifd.close();
			}
			await super.stop();
			throw ex;
		}
	}
	async stop(){
		if(this.eifd != null){
			await this.eifd.close();
		}
		if(this.pifd != null){
			await this.pifd.close();
		}
		await super.stop();
	}
	async getNode(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.nodeElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.nodeElementFinder.item(offsetRefIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const {nodes} = await this.readDecodedMapSegment(mapFileOffset);
		const nodeIndex = bounds.eq(nodes, {id}, (a, b) => a.id - b.id);
		if(nodeIndex === -1){
			return null;
		}
		return nodes[nodeIndex];
	}
	async getWay(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.wayElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.wayElementFinder.item(offsetRefIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const {ways} = await this.readDecodedMapSegment(mapFileOffset);
		const wayIndex = bounds.eq(ways, {id}, (a, b) => a.id - b.id);
		if(wayIndex === -1){
			return null;
		}
		return ways[wayIndex];
	}
	async getRelation(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.relationElementFinder.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.relationElementFinder.item(offsetRefIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const {relations} = await this.readDecodedMapSegment(mapFileOffset);
		const relationIndex = bounds.eq(relations, {id}, (a, b) => a.id - b.id);
		if(relationIndex === -1){
			return null;
		}
		return relations[relationIndex];
	}
	/**
	 * @async
	 * @param {number | import("./map-reader-base").OSMNode} nodeOrID
	 * @returns {Promise<Array<import("./map-reader-base").OSMWay>>}
	 */
	async getWayParentsFromNode(nodeOrID){
		const childID = typeof nodeOrID === "number" ? nodeOrID : nodeOrID.id;
		const childMapOffsetIndex = await this.nodeElementFinder.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.nodeElementFinder.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.nodeToWayFinder.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		let parentIndexOffset = (await this.nodeToWayFinder.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentListLength = (await this.pifd.read(
			Buffer.allocUnsafe(this._parentArrayLengthByteLength),
			0,
			this._parentArrayLengthByteLength,
			parentIndexOffset
		)).buffer.readUIntLE(0, this._parentArrayLengthByteLength);
		parentIndexOffset += this._parentArrayLengthByteLength;

		const parentListBuffer = (await this.pifd.read(
			Buffer.allocUnsafe(parentListLength * PARENT_INDEX_LIST_ITEM_SIZE),
			0,
			parentListLength * PARENT_INDEX_LIST_ITEM_SIZE,
			parentIndexOffset
		)).buffer;
		const results = [];
		for(let i = 0; i < parentListLength; i += 1){
			const smallestChildID = parentListBuffer.readUIntLE(i * PARENT_INDEX_LIST_ITEM_SIZE, INT48_SIZE);
			const largestChildID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE,
				INT48_SIZE
			);
			if(smallestChildID > childID){
				// It's sorted by smallest child ID, so impossible for more parents to be found at this point
				break;
			}
			if(largestChildID < childID){
				continue;
			}
			const parentMapOffset = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 2,
				INT48_SIZE
			);
			const smallestParentID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 3,
				INT48_SIZE
			);
			const largestParentID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 4,
				INT48_SIZE
			);
			const {ways} = await this.readDecodedMapSegment(parentMapOffset);
			for(let ii = bounds.eq(ways, {id: smallestParentID}, (a,b) => a.id - b.id); ii < ways.length; ii += 1){
				const way = ways[ii];
				if(way.nodes.indexOf(childID) != -1){
					results.push(way);
				}
				if(way.id == largestParentID){
					break;
				}
			}
		}
		return results;
	}
	/**
	 * @private
	 * @param {number} parentIndexOffset 
	 * @param {number} childID 
	 * @param {"node" | "way" | "relation"} childType
	 * @returns {Promise<Array<import("./map-reader-base").OSMRelation>>}
	 */
	async _getRelationParents(parentIndexOffset, childID, childType){
		const parentListLength = (await this.pifd.read(
			Buffer.allocUnsafe(this._parentArrayLengthByteLength),
			0,
			this._parentArrayLengthByteLength,
			parentIndexOffset
		)).buffer.readUIntLE(0, this._parentArrayLengthByteLength);
		parentIndexOffset += this._parentArrayLengthByteLength;

		const parentListBuffer = (await this.pifd.read(
			Buffer.allocUnsafe(parentListLength * PARENT_INDEX_LIST_ITEM_SIZE),
			0,
			parentListLength * PARENT_INDEX_LIST_ITEM_SIZE,
			parentIndexOffset
		)).buffer;
		const results = [];
		for(let i = 0; i < parentListLength; i += 1){
			const smallestChildID = parentListBuffer.readUIntLE(i * PARENT_INDEX_LIST_ITEM_SIZE, INT48_SIZE);
			const largestChildID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE,
				INT48_SIZE
			);
			if(smallestChildID > childID){
				// It's sorted by smallest child ID, so impossible for more parents to be found at this point
				break;
			}
			if(largestChildID < childID){
				continue;
			}
			const parentMapOffset = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 2,
				INT48_SIZE
			);
			const smallestParentID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 3,
				INT48_SIZE
			);
			const largestParentID = parentListBuffer.readUIntLE(
				i * PARENT_INDEX_LIST_ITEM_SIZE + INT48_SIZE * 4,
				INT48_SIZE
			);
			const {relations} = await this.readDecodedMapSegment(parentMapOffset);
			for(
				let ii = bounds.eq(relations, {id: smallestParentID}, (a,b) => a.id - b.id);
				ii < relations.length;
				ii += 1
			){
				const relation = relations[ii];
				if(relation.members.findIndex(member => member.id == childID && member.type == childType) != -1){
					results.push(relation);
				}
				if(relation.id == largestParentID){
					break;
				}
			}
		}
		return results;
	}
	/**
	 * @async
	 * @param {number | import("./map-reader-base").OSMNode} nodeOrID
	 * @returns {Promise<Array<import("./map-reader-base").OSMRelation>>}
	 */
	async getRelationParentsFromNode(nodeOrID){
		const childID = typeof nodeOrID === "number" ? nodeOrID : nodeOrID.id;
		const childMapOffsetIndex = await this.nodeElementFinder.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.nodeElementFinder.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.nodeToRelFinder.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.nodeToRelFinder.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		return this._getRelationParents(parentIndexOffset, childID, "node");
	}
	/**
	 * @async
	 * @param {number | import("./map-reader-base").OSMWay} wayOrID
	 * @returns {Promise<Array<import("./map-reader-base").OSMRelation>>}
	 */
	async getRelationParentsFromWay(wayOrID){
		const childID = typeof wayOrID === "number" ? wayOrID : wayOrID.id;
		const childMapOffsetIndex = await this.wayElementFinder.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.wayElementFinder.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.wayToRelFinder.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.wayToRelFinder.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		return this._getRelationParents(parentIndexOffset, childID, "way");
	}
	/**
	 * @async
	 * @param {number | import("./map-reader-base").OSMRelation} relationOrID
	 * @returns {Promise<Array<import("./map-reader-base").OSMRelation>>}
	 */
	async getRelationParentsFromRelation(relationOrID){
		const childID = typeof relationOrID === "number" ? relationOrID : relationOrID.id;
		const childMapOffsetIndex = await this.relationElementFinder.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.relationElementFinder.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.relToRelFinder.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.relToRelFinder.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		return this._getRelationParents(parentIndexOffset, childID, "relation");
	}
}

module.exports = {MapReader};