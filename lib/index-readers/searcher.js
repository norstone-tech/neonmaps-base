const path = require("path");
const {promises: fsp} = require("fs");
// based on https://github.com/mikolalysenko/binary-search-bounds
/**
 * Used for finding things within index files
 */
class NumericIndexFileSearcher{
	/**
	 * @param {string | fsp.FileHandle} file
	 * @param {number} blockSize 
	 * @param {number} intSize 
	 * @param {number} [intOffset=0] 
	 * @param {number} [blockStart=0] 
	 * @param {number} [blockEnd]
	 * @param {boolean} [bigEndian = false]
	 * @param {boolean} [cacheAmount=100000]
	 */
	constructor(file, blockSize, intSize, intOffset = 0, blockStart = 0, blockEnd, bigEndian = false, cacheAmount){
		if(typeof file === "string"){
			this.filePath = path.resolve(file);
		}else{
			this.fd = file;
		}
		this.maxCacheAmount = cacheAmount;
		/**@type {Map<number, Buffer | Promise<Buffer>}*/
		this.cache = new Map();
		/**@type {Map<number, number | Promise<number>}*/
		this.cacheValues = new Map();
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
			this.fd = await fsp.open(this.filePath); // Read only by default
		}
		try{
			if(this.options.blockEnd == null){
				this.options.blockEnd = (await fsp.stat(this.filePath)).size;
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
				await this.fd.close();
			}
			this.fd = null;
			throw ex;
		}
	}
	async stop(){
		if(this.filePath && this.fd != null){
			await this.fd.close();
			this.fd = null;
		}
	}
	async item(i){
		if(i >= this.length){
			throw new RangeError("NumericIndexFileSearcher.item: out of range");
		}
		if(this.cache.has(i)){
			const result = this.cache.get(i);
			this.cache.delete(i);
			this.cache.set(i, result);
			return result;
		}
		const {blockStart, blockSize} = this.options;
		const result = (async () => {
			return (
				await this.fd.read(Buffer.allocUnsafe(blockSize), 0, blockSize, blockStart + i * blockSize)
			).buffer;
		})();
		if(this.maxCacheAmount > 0){
			result.then(buf => this.cache.has(i) && this.cache.set(i, buf));
			this.cache.set(i, result);
			while(this.cache.size > this.maxCacheAmount){
				// Delete the first (and oldest) thing in the set
				this.cache.delete(this.cache.keys().next().value);
			}
		}
		return result;
	}
	async itemValue(i){
		if(this.cacheValues.has(i)){
			const result = this.cacheValues.get(i);
			this.cacheValues.delete(i);
			this.cacheValues.set(i, result);
			return result;
		}
		const {bigEndian, intOffset, intSize} = this.options;
		const result = (async () => {
			const buf = await this.item(i);
			return bigEndian ? buf.readUIntBE(intOffset, intSize) : buf.readUIntLE(intOffset, intSize);
		})();
		
		if(this.maxCacheAmount > 0){
			result.then(num => this.cacheValues.has(i) && this.cacheValues.set(i, num));
			this.cacheValues.set(i, result);
			while(this.cacheValues.size > this.maxCacheAmount){
				// Delete the first (and oldest) thing in the set
				this.cacheValues.delete(this.cacheValues.keys().next().value);
			}
		}
		return result;
	}
	itemValueSync(i){
		const result = this.cacheValues.get(i);
		if(result instanceof Promise){
			return;
		}
		if(result != null){
			this.cacheValues.delete(i);
			this.cacheValues.set(i, result);
		}
		return result;
	}
	async ge(val, l = 0, h = this.length - 1) {
		let m, x, p;
		let i = h + 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			x = this.itemValueSync(m) ?? await this.itemValue(m);
			p = x - val;
			if (p >= 0) { i = m; h = m - 1 } else { l = m + 1 }
		}
		return i;
	};
	
	async gt(val, l = 0, h = this.length - 1) {
		let m, x, p;
		let i = h + 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			x = this.itemValueSync(m) ?? await this.itemValue(m);
			p = x - val;
			if (p > 0) { i = m; h = m - 1 } else { l = m + 1 }
		}
		return i;
	};
	
	async lt(val, l = 0, h = this.length - 1) {
		let m, x, p;
		let i = l - 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			x = this.itemValueSync(m) ?? await this.itemValue(m);
			p = x - val;
			if (p < 0) { i = m; l = m + 1 } else { h = m - 1 }
		}
		return i;
	};
	
	async le(val, l = 0, h = this.length - 1) {
		let m, x, p;
		let i = l - 1;
		while (l <= h) {
			m = (l + h) >>> 1;
			x = this.itemValueSync(m) ?? await this.itemValue(m);
			p = x - val;
			if (p <= 0) { i = m; l = m + 1 } else { h = m - 1 }
		}
		return i;
	};

	async eq(val, l = 0, h = this.length - 1) {
		let m, x, p;
		while (l <= h) {
			m = (l + h) >>> 1;
			x = this.itemValueSync(m) ?? await this.itemValue(m);
			p = x - val;
			if (p === 0) { return m }
			if (p <= 0) { l = m + 1 } else { h = m - 1 }
		}
		return -1;
	}
}
module.exports = {NumericIndexFileSearcher};
