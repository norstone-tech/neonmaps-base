const {promises: fsp} = require("fs");
class CachedFileHandle {
	/**
	 * @param {string} path 
	 * @param {"r" | "r+" | "w" | "wx" | "w+" | "wx+"} [mode = "r"] 
	 * @param {number} [chunkSize=16384] 
	 * @param {number} [chunkAmount=16] 
	 */
	constructor(path, mode = "r", chunkSize = 16384, chunkAmount = 16){
		this.filePath = path;
		this.fileMode = mode;
		this.chunkSize = chunkSize;
		this.maxChunkAmount = chunkAmount;
		/**@type {Map<number, Buffer>} */
		this._chunks = new Map();
		// this._writing = mode !== "r";
	}
	async open(){
		this.fd = await fsp.open(this.filePath, this.fileMode);
	}
	async close(){
		if(this.fileMode != "r"){
			for(const offset of this._chunks.keys()){
				const buf = this._chunks.get(offset);
				await this.fd.write(buf, 0, buf.length, offset);
			}
		}
		this._chunks.clear();
		if(this.fd != null){
			this.fd.close();
			this.fd = null;
		}
	}
	_flushCache(){
		if(this._flushing){
			return;
		}
		this._flushing = (async () => {
			do{
				// Delete the first (and oldest) thing in the map
				const offset = this._chunks.keys().next().value;
				if(this.fileMode != "r"){
					const chunk = this._chunks.get(offset);
					// TODO: Perhaps only write when the chunk has changed?
					await this.fd.write(chunk, 0, chunk.length, offset);
				}
				this._chunks.delete(offset);
			}while(this._chunks.size > this.maxChunkAmount);
			delete this._flushing;
		})();
	}
	async _getChunk(offset){
		if(this._chunks.has(offset)){
			const result = this._chunks.get(offset);
			this._chunks.delete(offset);
			this._chunks.set(offset, result);
			return result;
		}
		let newChunk = Buffer.allocUnsafe(this.chunkSize);
		const {bytesRead} = await this.fd.read(newChunk, 0, this.chunkSize, offset);
		if(bytesRead < this.chunkSize){
			newChunk = newChunk.subarray(0, bytesRead);
		}
		this._chunks.set(offset, newChunk);
		while(this._chunks.size > this.maxChunkAmount){
			this._flushCache();
		}
		return newChunk;
	}
	/**
	 * @param {number} offset
	 * @param {Buffer} buffer
	 * @param {Promise<number>}
	 */
	async readToBuffer(offset, buffer){
		if(this._chunks.size == 3 && this._chunks.get(32768).length >= 16372 && this.filePath.endsWith("node_to_way")){
			debugger;
		}
		const fileChunkOffset = offset - (offset % this.chunkSize);
		const lastOffset = offset + buffer.length;
		const lastFileChunkOffset = lastOffset - (lastOffset % this.chunkSize);
		/**@type {Buffer} */
		let chunk;
		let bytesRead = 0;
		let sourceStart = offset - fileChunkOffset;
		let eof = false;
		for(let i = fileChunkOffset; i < lastFileChunkOffset; i += this.chunkSize){
			chunk = await this._getChunk(i);
			if(chunk.length < this.chunkSize){
				eof = true;
			}
			bytesRead += chunk.copy(buffer, bytesRead, sourceStart);
			sourceStart = 0;
		}
		if(!eof){
			chunk = await this._getChunk(lastFileChunkOffset);
			if(sourceStart >= chunk.length){
				debugger;
			}
			bytesRead += chunk.copy(buffer, bytesRead, sourceStart, sourceStart + buffer.length - bytesRead);
		}
		return bytesRead;
	}
	/**
	 * @param {number} offset 
	 * @param {number} size
	 * @param {Promise<Buffer>}
	 */
	async read(offset, size){
		const buffer = Buffer.allocUnsafe(size);
		const bytesRead = await this.readToBuffer(offset, buffer);
		if(bytesRead < size){
			return buffer.slice(0, bytesRead);
		}
		return buffer;
	}
	/**
	 * @param {number} offset
	 * @param {Buffer} buffer
	 * @returns {Promise<void>}
	 */
	async write(offset, buffer){
		if(this._chunks.size == 3 && this._chunks.get(32768).length >= 16372 && this.filePath.endsWith("node_to_way")){
			debugger;
		}
		if(buffer.length === 0){
			return;
		}
		if(this.fileMode == "r"){
			throw new Error("This CachedFileHandle is read only!");
		}
		if(this._flushing){
			await this._flushing;
		}
		const fileChunkOffset = offset - (offset % this.chunkSize);
		const lastOffset = offset + buffer.length;
		const lastFileChunkOffset = lastOffset - (lastOffset % this.chunkSize);
		if(fileChunkOffset == lastFileChunkOffset){
			const chunk = await this._getChunk(fileChunkOffset);
			const chunkOffset = offset - fileChunkOffset;
			const chunkCopyEnd = buffer.length + chunkOffset;
			if(chunk.length < chunkCopyEnd){
				const newChunk = Buffer.alloc(chunkCopyEnd);
				chunk.copy(newChunk);
				buffer.copy(newChunk, chunkOffset);
				this._chunks.set(fileChunkOffset, newChunk);
			}else{
				buffer.copy(chunk, chunkOffset);
			}
		}else{
			for(let i = fileChunkOffset; i <= lastFileChunkOffset; i += this.chunkSize){
				// It's all getting overwritten anyway
				this._chunks.delete(i);
			}
			await this.fd.write(buffer, 0, buffer.length, offset);
		}
	}
}
module.exports = {CachedFileHandle};
