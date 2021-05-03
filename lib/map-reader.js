const {promises: fsp} = require("fs");
const path = require("path");
const bounds = require('binary-search-bounds');
const {MapReaderBase} = require("./map-reader-base");
const {OSMElementIDIndexReader} = require("./index-readers/elem-id");
const {OSMElementParentIndexReader} = require("./index-readers/parents");

const INT48_SIZE = 6;
const PARENT_INDEX_LIST_ITEM_COUNT = 5;
const PARENT_INDEX_LIST_ITEM_SIZE = PARENT_INDEX_LIST_ITEM_COUNT * INT48_SIZE;

class MapReader extends MapReaderBase {
	/**
	 * @param {string} filePath
	 * @param {number} [rawCacheAmount=0]
	 * @param {number} [decodedCacheAmount=0]
	 * @param {number} [diskCacheSize=0]
	 * @param {boolean} [checksum=false]
	 * @param {boolean} [warnOnIndexFail=false]
	 */
	constructor(filePath, rawCacheAmount = 0, decodedCacheAmount = 0, diskCacheSize = 0, checksum){
		super(filePath, rawCacheAmount, decodedCacheAmount, diskCacheSize, checksum);
		this.elemIndex = new OSMElementIDIndexReader(filePath, this.checksum);
		this.parentIndex = new OSMElementParentIndexReader(filePath, this.checksum);
		this._warnOnIndexFail = warnOnIndexFail;
	}
	async init(){
		await super.init();
		try{
			try{
				await this.elemIndex.init();
				await this.parentIndex.init();
				this._parentArrayLengthByteLength = this.parentIndex._parentArrayLengthByteLength;
			}catch(ex){
				if(this._warnOnIndexFail){
					process.emit("warning", ex);
				}else{
					throw ex;
				}
			}
		}catch(ex){
			await this.elemIndex.stop();
			await this.parentIndex.stop();
			await super.stop();
			throw ex;
		}
	}
	async stop(){
		await this.elemIndex.stop();
		await this.parentIndex.stop();
		await super.stop();
	}
	async getNode(id){
		if(typeof id !== "number"){
			id = Number(id);
		}
		const offsetRefIndex = await this.elemIndex.nodeIndex.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.elemIndex.nodeIndex.item(offsetRefIndex)).readUIntLE(
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
		const offsetRefIndex = await this.elemIndex.wayIndex.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.elemIndex.wayIndex.item(offsetRefIndex)).readUIntLE(
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
		const offsetRefIndex = await this.elemIndex.relationIndex.le(id)
		if(offsetRefIndex === -1){
			return null;
		}
		const mapFileOffset = (await this.elemIndex.relationIndex.item(offsetRefIndex)).readUIntLE(
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
		const parentIndexFD = this.parentIndex.fd;
		const childID = typeof nodeOrID === "number" ? nodeOrID : nodeOrID.id;
		const childMapOffsetIndex = await this.elemIndex.nodeIndex.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.elemIndex.nodeIndex.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.parentIndex.nodeToWay.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		let parentIndexOffset = (await this.parentIndex.nodeToWay.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentListLength = (await parentIndexFD.read(
			Buffer.allocUnsafe(this._parentArrayLengthByteLength),
			0,
			this._parentArrayLengthByteLength,
			parentIndexOffset
		)).buffer.readUIntLE(0, this._parentArrayLengthByteLength);
		parentIndexOffset += this._parentArrayLengthByteLength;

		const parentListBuffer = (await parentIndexFD.read(
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
		const parentIndexFD = this.parentIndex.fd;
		const parentListLength = (await parentIndexFD.read(
			Buffer.allocUnsafe(this._parentArrayLengthByteLength),
			0,
			this._parentArrayLengthByteLength,
			parentIndexOffset
		)).buffer.readUIntLE(0, this._parentArrayLengthByteLength);
		parentIndexOffset += this._parentArrayLengthByteLength;

		const parentListBuffer = (await parentIndexFD.read(
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
		const childMapOffsetIndex = await this.elemIndex.nodeIndex.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.elemIndex.nodeIndex.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.parentIndex.nodeToRel.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.parentIndex.nodeToRel.item(parentIndexOffsetIndex)).readUIntLE(
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
		const childMapOffsetIndex = await this.elemIndex.wayIndex.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.elemIndex.wayIndex.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.parentIndex.wayToRel.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.parentIndex.wayToRel.item(parentIndexOffsetIndex)).readUIntLE(
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
		const childMapOffsetIndex = await this.elemIndex.relationIndex.le(childID);
		if(childMapOffsetIndex === -1){
			return [];
		}
		const childMapOffset = (await this.elemIndex.relationIndex.item(childMapOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		const parentIndexOffsetIndex = await this.parentIndex.relToRel.eq(childMapOffset);
		if(parentIndexOffsetIndex === -1){
			return [];
		}
		const parentIndexOffset = (await this.parentIndex.relToRel.item(parentIndexOffsetIndex)).readUIntLE(
			INT48_SIZE,
			INT48_SIZE
		);
		return this._getRelationParents(parentIndexOffset, childID, "relation");
	}
}
module.exports = {MapReader};
