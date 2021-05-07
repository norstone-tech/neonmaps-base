const {promises: fsp} = require("fs");
const turf = require("@turf/helpers");
const bounds = require('binary-search-bounds');
const {MapReaderBase} = require("./map-reader-base");
const {OSMElementIDIndexReader} = require("./index-readers/elem-id");
const {OSMElementParentIndexReader} = require("./index-readers/parents");
const {MapGeometryReader} = require("./index-readers/geometry");

const INT48_SIZE = 6;
const PARENT_INDEX_LIST_ITEM_COUNT = 5;
const PARENT_INDEX_LIST_ITEM_SIZE = PARENT_INDEX_LIST_ITEM_COUNT * INT48_SIZE;
/**
 * @typedef MultipolygonMember
 * @property {boolean} inner
 * @property {Array<Array<number>>} points
*/

class MapReader extends MapReaderBase {
	/**
	 * @param {string} filePath
	 * @param {number} [rawCacheAmount=0]
	 * @param {number} [decodedCacheAmount=0]
	 * @param {number} [diskCacheSize=0]
	 * @param {number} [wayGeoCacheAmount=10]
	 * @param {number} [relationGeoCacheAmount=5]
	 * @param {boolean} [checksum=false]
	 * @param {boolean} [warnOnIndexFail=false]
	 */
	constructor(
		filePath,
		rawCacheAmount = 0,
		decodedCacheAmount = 0,
		diskCacheSize = 0,
		wayGeoCacheAmount = 10,
		relationGeoCacheAmount = 5,
		checksum,
		warnOnIndexFail
	){
		super(filePath, rawCacheAmount, decodedCacheAmount, diskCacheSize, checksum);
		this.filePath = filePath;
		this.elemIndex = new OSMElementIDIndexReader(filePath, this.checksum);
		this.parentIndex = new OSMElementParentIndexReader(filePath, this.checksum);
		this._warnOnIndexFail = warnOnIndexFail;
		this._wayGeoCacheAmount = wayGeoCacheAmount;
		this._relationGeoCacheAmount = relationGeoCacheAmount;
	}
	async init(){
		await super.init();
		try{
			try{
				await this.elemIndex.init();
				await this.parentIndex.init();
				this._parentArrayLengthByteLength = this.parentIndex._parentArrayLengthByteLength;
				this.geometryReader = new MapGeometryReader(
					this.filePath,
					this.elemIndex,
					this._wayGeoCacheAmount,
					this._relationGeoCacheAmount,
					this.checksum
				);
				await this.geometryReader.init();
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
	/**
	 * @async
	 * Get way geography in [lat, lon] note that in a closed object, the last array will be the same object as the
	 * first
	 * @param {number} wayID
	 * @returns {Promise<Array<Array<number>>>}
	 */
	async getWayGeometry(wayID){
		const rawGeoResult = await this.geometryReader.getWayGeometry(wayID);
		if(rawGeoResult == null){
			return null;
		}
		const {geometry: rawGeometry} = rawGeoResult;
		const result = rawGeometry.lat.map((lat, i) => {
			return [rawGeometry.lon[i] / 1000000000, lat / 1000000000];
		});
		if(rawGeometry.closed){
			result.push(result[0]);
		}
		return result;
	}
	/**
	 * @async
	 * @param {number} relID
	 * @returns {Promise<Array<MultipolygonMember>>}
	 */
	async getRelationGeometry(relID){
		const rawGeoResult = await this.geometryReader.getRelationGeometry(relID);
		if(rawGeoResult == null){
			return null;
		}
		const {geometry: rawGeometry} = rawGeoResult;
		return rawGeometry.map(rawGeo => {
			const points = rawGeo.lat.map((lat, i) => {
				return [rawGeo.lon[i] / 1000000000, lat / 1000000000];
			});
			if(rawGeo.closed){
				points.push(points[0]);
			}
			return {
				inner: rawGeo.inner,
				points
			};
		});
	}
	/**
	 * @async
	 * Convert the specified node to a GeoJSON feature, feature properties will be the node's tags
	 * @param {import("./map-reader-base").OSMNode | number} node OSMNode or NodeID
	 * @returns {Promise<turf.Feature<turf.Point, Object<string, string>>>}
	 */
	async getNodeGeoJSON(node){
		if(typeof node === "number"){
			node = await this.getNode(node);
		}
		return turf.point([node.lon, node.lat], node.tags.toJSON());
	}
	/**
	 * @async
	 * Convert the specified way to a GeoJSON feature, feature properties will be the way's tags.
	 * Open ways will return a LineString, closed ways will return a Polygon
	 * @param {import("./map-reader-base").OSMWay | number} way OSMNode or NodeID
	 * @returns {Promise<turf.Feature<turf.LineString | turf.Polygon, Object<string, string>>>}
	 */
	async getWayGeoJSON(way){
		if(typeof way === "number"){
			way = await this.getWay(way);
		}
		if(way == null){
			return null;
		}
		const wayGeo = await this.getWayGeometry(way.id);
		if(wayGeo == null){
			return null;
		}
		if(wayGeo[0] == wayGeo[wayGeo.length - 1]){
			return turf.polygon([wayGeo], way.tags.toJSON());
		}else{
			return turf.lineString(wayGeo, way.tags.toJSON());
		}
	}
	/**
	 * @async
	 * Convert the specified way to a GeoJSON feature, feature properties will be the way's tags.
	 * Open ways will return a LineString, closed ways will return a Polygon
	 * @param {import("./map-reader-base").OSMRelation | number} relation OSMNode or NodeID
	 * @returns {Promise<turf.Feature<turf.LineString | turf.MultiLineString | turf.Polygon | turf.MultiPolygon, Object<string, string>>>}
	 */
	async getRelationGeoJSON(relation){
		if(typeof relation === "number"){
			relation = await this.getRelation(relation);
		}
		if(relation == null){
			return null;
		}
		const relGeo = await this.getRelationGeometry(relation.id);
		if(relGeo == null){
			return null;
		}
		let polygonIndex = 0;
		let polygons = [relGeo[0].points];
		switch(relation.tags.type){
			case "multipolygon":
			case "boundary":
				for(let i = 1; i < relGeo.length; i += 1){
					const relGeoMember = relGeo[i];
					if(!relGeoMember.inner){
						polygonIndex += 1;
						polygons[polygonIndex] = [];
					}
					polygons[polygonIndex].push(relGeoMember.points);
				}
				if(polygons.length == 1){
					return turf.polygon(polygons[0], relation.tags.toJSON());
				}else{
					return turf.multiPolygon(polygons, relation.tags.toJSON());
				}
				break;
			case "route":
				if(relGeo.length == 1){
					return turf.lineString(relGeo[0].points, relation.tags.toJSON());
				}else{
					return turf.multiLineString(relGeo.map(({points}) => points), relation.tags.toJSON());
				}
			default:
				return null;
		}
	}
}
module.exports = {MapReader};
