/**
 * Copyright (c) 2024 Danny Deschenes
 */

package com.github.ddeschenes_1.ragzip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.function.IntSupplier;

class TestUtils {
	
	@SuppressWarnings({"serial" })
	static class DataGen implements IntSupplier, Serializable {
		static final byte[] DATA = "abcdefghij__klmnopqrst___uvwxyz\n".getBytes(StandardCharsets.ISO_8859_1);
		Random r;
		int i;
		
		DataGen(Random r) {
			this.r = serClone(r); //we can't obtain the seed to start at same place, so we clone it instead.
		}
		@Override
		public int getAsInt() {
			if(r!=null)
				return r.nextInt() & 0xff;
			return DATA[i++ % DATA.length];
		}
	}
	
	static <T> T serClone(T obj) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			oos.flush();
			
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			
			@SuppressWarnings("unchecked")
			T ans = (T)ois.readObject();
			
			return ans;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static File generateRagzip(Long randomSeed, int pgSizeExponent, int idxSizeExponent, int size, boolean useFileChannel) throws IOException {
		Random r = randomSeed!=null ? new Random(randomSeed) : null;
		final IntSupplier dataGen0 = new TestUtils.DataGen(r);
		
		File f = File.createTempFile("test.dat.", ".rgz", new File("."));
		if(useFileChannel) {
			try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
				fc.truncate(0);
				try (RagzipOutputStream zos = new RagzipOutputStream(fc, pgSizeExponent, idxSizeExponent)) {
					fillExt(zos);
					fill(zos, dataGen0, size);
				}
			}
		} else {
			try (FileOutputStream fos = new FileOutputStream(f)) {
				try (RagzipOutputStream zos = new RagzipOutputStream(fos, pgSizeExponent, idxSizeExponent)) {
					fillExt(zos);
					fill(zos, dataGen0, size);
				}
			}
		}
		return f;
	}
	
	static void fillExt(RagzipOutputStream ragzos) throws IOException {
		ragzos.appendExtension(Extension.ofCustom(0xa, 1001, "my extension 1001"));
		ragzos.appendExtension(Extension.ofCustom(0xb, 1002, "my extension 1002".getBytes(StandardCharsets.UTF_8)));
		ragzos.appendExtension(Extension.ofSpec(3, "spec extension 3".getBytes(StandardCharsets.UTF_8)));
	}
	
	static void fill(OutputStream ragzos, IntSupplier dataGen, int size) throws IOException {
		byte[] ba = new byte[1<<12];
		while(size>0) {
			int n = Math.min(ba.length, size);
			for(int k=0; k<n; k++) {
				ba[k] = (byte)dataGen.getAsInt();
			}
			ragzos.write(ba, 0, n);
			size -= n;
		}
	}
	
}
