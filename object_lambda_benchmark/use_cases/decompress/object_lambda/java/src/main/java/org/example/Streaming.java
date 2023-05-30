package org.example;

import com.amazonaws.services.lambda.runtime.events.S3ObjectLambdaEvent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.WriteGetObjectResponseRequest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.zip.CheckedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

public class Streaming {

    public void handleRequest(S3ObjectLambdaEvent event, Context context) throws Exception {
        AmazonS3 s3Client = AmazonS3Client.builder().build();
        HttpClient httpClient = HttpClient.newBuilder().build();

        // Request the original object from S3.
        var presignedResponse = httpClient.send(
                HttpRequest.newBuilder(new URI(event.inputS3Url())).GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());

        // Consume the incoming response body from the presigned request,
        // apply our transformation on that data, and emit the transformed bytes
        // into the body of the WriteGetObjectResponse request as soon as they're ready.
        // This example compresses the data from S3, but any processing pertinent
        // to your application can be performed here.
        var bodyStream = new GZIPCompressorInputStream(presignedResponse.body());

        // Stream the bytes back to the caller.
        s3Client.writeGetObjectResponse(new WriteGetObjectResponseRequest()
                .withRequestRoute(event.outputRoute())
                .withRequestToken(event.outputToken())
                .withInputStream(bodyStream));
    }


    /**
     * Implements an input stream for compressing input data in the GZIP compression format.
     */
    class GZIPCompressorInputStream extends DeflaterInputStream {
        // GZIP header magic number.
        private final static int GZIP_MAGIC = 0x8b1f;

        // Writes GZIP member header.
        private final byte[] HEADER = {
                (byte) GZIP_MAGIC,                // Magic number (short)
                (byte) (GZIP_MAGIC >> 8),          // Magic number (short)
                Deflater.DEFLATED,                // Compression method (CM)
                0,                                // Flags (FLG)
                0,                                // Modification time MTIME (int)
                0,                                // Modification time MTIME (int)
                0,                                // Modification time MTIME (int)
                0,                                // Modification time MTIME (int)
                0,                                // Extra flags (XFLG)
                0                                 // Operating system (OS)
        };

        // Trailer length in bytes.
        private final static int TRAILER_LENGTH = 8;

        // If true, the GZIP trailer has been written.
        private boolean trailerWritten = false;

        // Internal buffer for GZIP header and trailer.
        private Buffer buffer;

        /**
         * Creates a new {@link GZIPCompressorInputStream} from an uncompressed {@link InputStream}.
         *
         * @param in The uncompressed {@link InputStream}.
         */
        public GZIPCompressorInputStream(InputStream in) {
            super(new CheckedInputStream(in, new CRC32()), new Deflater(Deflater.DEFAULT_COMPRESSION, true));
            buffer = new Buffer();
        }

        public int read(byte b[], int off, int len) throws IOException {
            // Check if there are bytes left to be read from the internal buffer. This is used to provide the header
            // or trailer, and always takes precedence.
            int count;
            if (buffer.getByteCountRemainder() > 0) {
                // Write data from the internal buffer into b.
                count = Math.min(len, buffer.getByteCountRemainder());
                System.arraycopy(buffer.data, buffer.position, b, off, count);

                // Advance the internal buffer position as "count" bytes have already been read.
                buffer.position += count;
                return count;
            }

            // Attempt to read compressed input data.
            count = super.read(b, off, len);
            if (count > 0) {
                return count;
            }

            /* If the stream has reached completion, write out the GZIP trailer and re-attempt
             * the read */
            if (count <= 0 && !trailerWritten) {
                buffer.position = 0;
                buffer.length = writeTrailer(buffer.data, buffer.position);
                trailerWritten = true;
                return read(b, off, len);
            } else {
                return count;
            }
        }

        /**
         * Writes GZIP member trailer to a byte array, starting at a given offset.
         *
         * @param buf    The buffer to write the trailer to.
         * @param offset The offset from which to start writing.
         * @return The amount of bytes that were written.
         * @throws IOException If an I/O error is produced.
         */
        private int writeTrailer(byte[] buf, int offset) throws IOException {
            int count = writeInt((int) ((CheckedInputStream) this.in).getChecksum().getValue(), buf, offset); // CRC-32 of uncompr. data
            count += writeInt(def.getTotalIn(), buf, offset + 4); // Number of uncompr. bytes
            return count;
        }

        /**
         * Writes integer in Intel byte order to a byte array, starting at a given offset.
         *
         * @param i      The integer to write.
         * @param buf    The buffer to write the integer to.
         * @param offset The offset from which to start writing.
         * @return The amount of bytes written.
         * @throws IOException If an I/O error is produced.
         */
        private int writeInt(int i, byte[] buf, int offset) throws IOException {
            int count = writeShort(i & 0xffff, buf, offset);
            count += writeShort((i >> 16) & 0xffff, buf, offset + 2);
            return count;
        }

        /**
         * Writes short integer in Intel byte order to a byte array, starting at a given offset.
         *
         * @param s      The short to write.
         * @param buf    The buffer to write the integer to.
         * @param offset The offset from which to start writing.
         * @return The amount of bytes written.
         * @throws IOException If an I/O error is produced.
         */
        private int writeShort(int s, byte[] buf, int offset) throws IOException {
            buf[offset] = (byte) (s & 0xff);
            buf[offset + 1] = (byte) ((s >> 8) & 0xff);
            return 2;
        }

        /**
         * Helper inner class containing the length and position of the internal buffer.
         */
        private class Buffer {
            public byte[] data;
            public int position;
            public int length;

            /**
             * Creates a new buffer initializing it with the GZIP header content.
             */
            public Buffer() {
                data = new byte[Math.max(HEADER.length, TRAILER_LENGTH)];
                System.arraycopy(HEADER, 0, data, 0, HEADER.length);
                position = 0;
                length = HEADER.length;
            }

            /**
             * Returns the amount of bytes that are left to be read from the buffer.
             *
             * @return The byte counts to be read.
             */
            int getByteCountRemainder() {
                return length - position;
            }
        }
    }

}