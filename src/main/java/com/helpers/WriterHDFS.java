package com.helpers;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Writer HDFS
 * Classe para escrever fluxos de caracteres em arquivos no HDFS. 
 */
public class WriterHDFS {

	private FSDataOutputStream out;

	/**
	 * Se o caminho para o arquivo de destino não existir, ele será criado.
	 * @param path destination file path.
	 * @throws IOException
	 */
	public WriterHDFS(String path) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path pt = new Path("hdfs:" + path);
		if(fs.exists(pt)) {
			this.out = fs.append(pt);
		}
		else {
			this.out = fs.create(pt);
		}
	}

	public void write(String str) throws IOException {
		this.out.writeBytes(str);
	}

	public void close() throws IOException {
		this.out.close();
	}
}
