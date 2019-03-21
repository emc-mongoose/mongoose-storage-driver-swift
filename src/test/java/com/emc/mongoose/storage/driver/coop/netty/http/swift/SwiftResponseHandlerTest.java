package com.emc.mongoose.storage.driver.coop.netty.http.swift;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AttributeKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author veronika K. on 28.02.19
 */
public class SwiftResponseHandlerTest {

	private static final String HTTP_RESPONSE_START = ""
					+ "\r\n--3d07fbbddf4041880c931c29e43cb6c4"
					+ "\r\nContent-Type: application/octet-stream"
					+ "\r\nContent-Range: bytes 0-4/10\n\r\n\r";

	private static final String HTTP_RESPONSE_END = ""
					+ "\r\n--3d07fbbddf4041880c931c29e43cb6c4--\r\n";

	private static final String HTTP_RESPONSE = ""
					+ HTTP_RESPONSE_START
					+ "\naaa\naa"
					+ "\r\n--3d07fbbddf4041880c931c29e43cb6c4"
					+ "\r\nContent-Type: application/octet-stream"
					+ "\r\nContent-Range: bytes 5-9/10\n\r\n\r"
					+ "aaaaa"
					+ HTTP_RESPONSE_END;

	private static final String PART_1_HTTP_RESPONSE = ""
					+ HTTP_RESPONSE_START
					+ "\naaa\naa"
					+ "\r\n--3d07fbbddf4041880c931c29e43cb6c4"
					+ "\r\nContent-Type: appli";

	private static final String PART_2_HTTP_RESPONSE = ""
					+ "cation/octet-stream"
					+ "\r\nContent-Range: bytes 5-9/10\n\r\n\r"
					+ "aaaaa"
					+ HTTP_RESPONSE_END;

	private static final String EXPECTED_CONTENT = "\naaa\naaaaaaa";
	private static final String BOUNDARY = "--3d07fbbddf4041880c931c29e43cb6c4";
	private static final EmbeddedChannel CHANNEL = new EmbeddedChannel(); // CHANNEL mock
	private static final AttributeKey<String> ATTR_KEY_BOUNDARY_MARKER = AttributeKey
					.valueOf("boundary_marker");
	private static final AttributeKey<String> ATTR_KEY_CUT_CHUNK = AttributeKey
					.valueOf("cut_chunk");
	private static final SwiftResponseHandler RESPONSE_HANDLER = new SwiftResponseHandler(null,
					true);

	@Before
	public void setUp() {
		CHANNEL.attr(ATTR_KEY_BOUNDARY_MARKER).set(BOUNDARY);
		CHANNEL.attr(ATTR_KEY_CUT_CHUNK).set("");
	}

	private ByteBuf readFromChannel(final EmbeddedChannel channel) {
		final var readed = channel.readOutbound();
		ByteBuf result;
		try {
			result = (ByteBuf) readed;
		} catch (final ClassCastException ex) {
			result = Unpooled.copiedBuffer(readed.toString().getBytes());
		}
		return result;
	}

	private void assertEqualsByBytes(final ByteBuf expectedContent,
					final ByteBuf actualContent) {
		while (expectedContent.isReadable()) {
			final var a = expectedContent.readByte();
			final var b = actualContent.readByte();
			Assert.assertEquals(a, b);
		}
	}

	@Test
	public void fullContentTest() throws IOException {
		CHANNEL.writeOutbound(HTTP_RESPONSE);
		final var expectedContent = Unpooled.copiedBuffer(EXPECTED_CONTENT.getBytes());
		final var contentChunk = readFromChannel(CHANNEL);
		final var newContentChunk = RESPONSE_HANDLER.removeHeaders(CHANNEL, contentChunk);

		Assert.assertEquals(expectedContent.array().length, newContentChunk.array().length);
		assertEqualsByBytes(expectedContent, newContentChunk);
	}

	@Test
	public void unicodeContentTest() throws IOException {
		final var expectedContent = Unpooled.copiedBuffer(new byte[]{-3, -17, 10, -3, -10
		});
		CHANNEL.writeOutbound(HTTP_RESPONSE_START);
		CHANNEL.writeOutbound(expectedContent);
		CHANNEL.writeOutbound(HTTP_RESPONSE_END);
		final var rawActualContent = Unpooled.copiedBuffer(readFromChannel(CHANNEL),
						readFromChannel(CHANNEL),
						readFromChannel(CHANNEL));
		final var actualContent = RESPONSE_HANDLER.removeHeaders(CHANNEL,  rawActualContent);

		Assert.assertEquals(expectedContent.array().length, actualContent.array().length);
		assertEqualsByBytes(expectedContent, actualContent);
	}

	@Test
	public void partContentTest() throws IOException {
		final var expectedContent = Unpooled.copiedBuffer(EXPECTED_CONTENT.getBytes());

		CHANNEL.writeOutbound(PART_1_HTTP_RESPONSE);
		final var contentChunk1 = readFromChannel(CHANNEL);
		final var newContentChunk1 = RESPONSE_HANDLER.removeHeaders(CHANNEL, contentChunk1);

		CHANNEL.writeOutbound(PART_2_HTTP_RESPONSE);
		final var contentChunk2 = readFromChannel(CHANNEL);
		final var newContentChunk2 = RESPONSE_HANDLER.removeHeaders(CHANNEL, contentChunk2);

		final var fullContentChunk = Unpooled.copiedBuffer(newContentChunk1, newContentChunk2);

		Assert.assertEquals(expectedContent.array().length, fullContentChunk.array().length);
		assertEqualsByBytes(expectedContent, fullContentChunk);
	}
}
