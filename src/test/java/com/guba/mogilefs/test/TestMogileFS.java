/*
 * Created on Jun 27, 2005
 *
 * copyright ill.com 2005
 */
package com.guba.mogilefs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.guba.mogilefs.MogileException;
import com.guba.mogilefs.MogileFS;
import com.guba.mogilefs.PooledMogileFSImpl;

/**
 * @author ekohl
 */
public class TestMogileFS {
	private MogileFS mfs;
	private File file;

	private static final String DOMAIN = "test";
	private static final String STORAGE_CLASS = "oneDeviceTest";
	private static final String[] TRACKER = { "my.tracker.host.example.com:6001" };

	@Before
	public void setUp() throws IOException, MogileException {
		// Reload before each test to ensure each test is unique
		mfs = new PooledMogileFSImpl(DOMAIN, TRACKER, -1, 2, 10000);

		// Create a temporary file
		file = File.createTempFile("MogileFS-test", ".txt");
		file.deleteOnExit();

		// Write some random data to the file
		OutputStreamWriter stream = new OutputStreamWriter(
				new FileOutputStream(file));
		stream.write(Long.toHexString(Double.doubleToLongBits(Math.random())));
		stream.close();
	}

	/**
	 * Test {@link MogileFS#newFile(String, String, long)} by attempting to
	 * upload {@value #LOCAL_FILENAME}. Then it is retrieved and compared. To
	 * close it up, the file is deleted.
	 * 
	 * @throws IOException
	 * @throws MogileException
	 */
	@Test
	public void testMogileFS() throws IOException, MogileException {
		// Some variables
		InputStream localFile = null;
		InputStream input = null;
		OutputStream output = null;

		// Step 1: upload the local file
		try {
			// Open the local file
			localFile = new FileInputStream(file);

			// Open the output file
			output = mfs.newFile(file.getName(), STORAGE_CLASS, file.length());
			Assert.assertNotNull("Outputstream null", output);

			// Create some helper variables
			byte[] buffer = new byte[1024];
			int count = 0;
			long totalCount = 0;

			// Actually upload
			while ((count = localFile.read(buffer)) >= 0) {
				output.write(buffer, 0, count);
				totalCount += count;
			}
			
			// Verify more than 0 bytes 
			Assert.assertEquals("Upload size is not equal to file size", file.length(), totalCount);
		} finally {
			// Close the output
			if (output != null)
				output.close();
			output = null;

			// Close the local file
			if (localFile != null)
				localFile.close();
			localFile = null;
		}

		// Step 2: Retrieve the remote file and compare it byte by byte with the
		// local file
		try {
			// Open the local file
			localFile = new FileInputStream(file);

			// Open the input stream to download
			input = mfs.getFileStream(file.getName());
			Assert.assertNotNull("Inputstream null", input);

			// Create some help variables
			byte[] localFileBuffer = new byte[1024];
			byte[] inputBuffer = new byte[localFileBuffer.length];
			int count = 0;
			long totalCount = 0;

			// Actually download and compare
			while ((count = localFile.read(localFileBuffer)) >= 0) {
				input.read(inputBuffer, 0, count);
				Assert.assertTrue(Arrays.equals(localFileBuffer, inputBuffer));
				totalCount += count;
			}
			
			// Verify that something was downloaded
			Assert.assertEquals("Download size is not equal to filesize", file.length(), totalCount);
		} finally {
			// Close the input
			if (input != null)
				input.close();
			input = null;

			// Reset the local file
			if (localFile != null)
				localFile.close();
			localFile = null;
		}

		// Step 3: remove the remotely created file
		mfs.delete(file.getName());
	}
}
