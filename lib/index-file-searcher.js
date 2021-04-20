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
	 */
	constructor(file, blockSize, intSize, intOffset = 0, blockStart = 0, blockEnd, bigEndian = false){
		if(typeof file === "string"){
			this.filePath = path.resolve(file);
		}else{
			this.fd = file;
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
		const {blockStart, blockSize} = this.options;
		return (await this.fd.read(Buffer.allocUnsafe(blockSize), 0, blockSize, blockStart + i * blockSize)).buffer
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
