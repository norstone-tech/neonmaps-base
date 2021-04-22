const path = require("path");
const {CachedFileHandle} = require("./cached-file-handle");
// based on https://github.com/mikolalysenko/binary-search-bounds
/**
 * Used for finding things within index files
 */
class NumericIndexFileSearcher{
	/**
	 * @param {string | CachedFileHandle} file
	 * @param {number} blockSize 
	 * @param {number} intSize 
	 * @param {number} [intOffset=0] 
	 * @param {number} [blockStart=0] 
	 * @param {number} [blockEnd]
	 * @param {boolean} [bigEndian = false]
	 */
	constructor(file, blockSize, intSize, intOffset = 0, blockStart = 0, blockEnd, bigEndian = false){
		if(typeof file === "string"){
			this.filePath = path.resolve(file);
			this.cfh = new CachedFileHandle(this.filePath);
		}else{
			this.cfh = file;
		}
		this.options = {
			blockSize,
			intSize,
			intOffset,
			blockStart,
			blockEnd,
			bigEndian
		};
	}
	async init(){
		if(this.filePath){
			this.cfh.open();
		}
		try{
			if(this.options.blockEnd == null){
				this.options.blockEnd = (await this.cfh.fd.stat()).size;
			}
			const {
				blockStart,
				blockEnd,
				blockSize
			} = this.options;
			if(((blockEnd - blockStart) % blockSize) != 0){
				throw new Error("(blockEnd - blockStart) must be a multiple of blockSize");
			}
			this.length = (blockEnd - blockStart) / blockSize;
		}catch(ex){
			if(this.filePath){
				await this.cfh.close();
			}
			this.cfh = null;
			throw ex;
		}
	}
	async stop(){
		if(this.filePath && this.cfh != null){
			await this.cfh.close();
			this.cfh = null;
		}
	}
	item(i){
		if(i >= this.length){
			return Promise.reject(new RangeError("NumericIndexFileSearcher.item: out of range"));
		}
		const {blockStart, blockSize} = this.options;
		return this.cfh.read(blockStart + i * blockSize, blockSize);
	}
	async ge(val) {
		const {bigEndian, intOffset, intSize} = this.options;
		let m, x, p, l = 0, h = this.length - 1;
		let i = h + 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			if(bigEndian){
				x = (await this.item(m)).readUIntBE(intOffset, intSize);
			}else{
				x = (await this.item(m)).readUIntLE(intOffset, intSize);
			}
			p = x - val;
			if (p >= 0) { i = m; h = m - 1 } else { l = m + 1 }
		}
		return i;
	};
	
	async gt(val) {
		const {bigEndian, intOffset, intSize} = this.options;
		let m, x, p, l = 0, h = this.length - 1;
		let i = h + 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			if(bigEndian){
				x = (await this.item(m)).readUIntBE(intOffset, intSize);
			}else{
				x = (await this.item(m)).readUIntLE(intOffset, intSize);
			}
			p = x - val;
			if (p > 0) { i = m; h = m - 1 } else { l = m + 1 }
		}
		return i;
	};
	
	async lt(val) {
		const {bigEndian, intOffset, intSize} = this.options;
		let m, x, p, l = 0, h = this.length - 1;
		let i = l - 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			if(bigEndian){
				x = (await this.item(m)).readUIntBE(intOffset, intSize);
			}else{
				x = (await this.item(m)).readUIntLE(intOffset, intSize);
			}
			p = x - val;
			if (p < 0) { i = m; l = m + 1 } else { h = m - 1 }
		}
		return i;
	};
	
	async le(val) {
		const {bigEndian, intOffset, intSize} = this.options;
		let m, x, p, l = 0, h = this.length - 1;
		let i = l - 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			if(bigEndian){
				x = (await this.item(m)).readUIntBE(intOffset, intSize);
			}else{
				x = (await this.item(m)).readUIntLE(intOffset, intSize);
			}
			p = x - val;
			if (p <= 0) { i = m; l = m + 1 } else { h = m - 1 }
		}
		return i;
	};

	async eq(val) {
		const {bigEndian, intOffset, intSize} = this.options;
		let m, x, p, l = 0, h = this.length - 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			if(bigEndian){
				x = (await this.item(m)).readUIntBE(intOffset, intSize);
			}else{
				x = (await this.item(m)).readUIntLE(intOffset, intSize);
			}
			p = x - val;
			if (p === 0) { return m }
			if (p <= 0) { l = m + 1 } else { h = m - 1 }
		}
		return -1;
	}
}
module.exports = {NumericIndexFileSearcher};
