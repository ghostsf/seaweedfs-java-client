package net.anumbrella.seaweedfs.core;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ContentType implements Serializable {

    private static final long serialVersionUID = -7768694718232371896L;

    // constants
    public static final ContentType APPLICATION_ATOM_XML = create(
            "application/atom+xml", StandardCharsets.ISO_8859_1);
    public static final ContentType APPLICATION_FORM_URLENCODED = create(
            "application/x-www-form-urlencoded", StandardCharsets.ISO_8859_1);
    public static final ContentType APPLICATION_JSON = create(
            "application/json", StandardCharsets.UTF_8);
    public static final ContentType APPLICATION_OCTET_STREAM = create(
            "application/octet-stream", (Charset) null);
    public static final ContentType APPLICATION_SOAP_XML = create(
            "application/soap+xml", StandardCharsets.UTF_8);
    public static final ContentType APPLICATION_SVG_XML = create(
            "application/svg+xml", StandardCharsets.ISO_8859_1);
    public static final ContentType APPLICATION_XHTML_XML = create(
            "application/xhtml+xml", StandardCharsets.ISO_8859_1);
    public static final ContentType APPLICATION_XML = create(
            "application/xml", StandardCharsets.ISO_8859_1);
    public static final ContentType IMAGE_BMP = create(
            "image/bmp");
    public static final ContentType IMAGE_GIF= create(
            "image/gif");
    public static final ContentType IMAGE_JPEG = create(
            "image/jpeg");
    public static final ContentType IMAGE_PNG = create(
            "image/png");
    public static final ContentType IMAGE_SVG= create(
            "image/svg+xml");
    public static final ContentType IMAGE_TIFF = create(
            "image/tiff");
    public static final ContentType IMAGE_WEBP = create(
            "image/webp");
    public static final ContentType MULTIPART_FORM_DATA = create(
            "multipart/form-data", StandardCharsets.ISO_8859_1);
    public static final ContentType TEXT_HTML = create(
            "text/html", StandardCharsets.ISO_8859_1);
    public static final ContentType TEXT_PLAIN = create(
            "text/plain", StandardCharsets.ISO_8859_1);
    public static final ContentType TEXT_XML = create("text/xml", StandardCharsets.ISO_8859_1);
    public static final ContentType WILDCARD = create(
            "*/*", (Charset) null);


    private static final Map<String, ContentType> CONTENT_TYPE_MAP;
    static {

        final ContentType[] contentTypes = {
                APPLICATION_ATOM_XML,
                APPLICATION_FORM_URLENCODED,
                APPLICATION_JSON,
                APPLICATION_SVG_XML,
                APPLICATION_XHTML_XML,
                APPLICATION_XML,
                IMAGE_BMP,
                IMAGE_GIF,
                IMAGE_JPEG,
                IMAGE_PNG,
                IMAGE_SVG,
                IMAGE_TIFF,
                IMAGE_WEBP,
                MULTIPART_FORM_DATA,
                TEXT_HTML,
                TEXT_PLAIN,
                TEXT_XML };
        final HashMap<String, ContentType> map = new HashMap<String, ContentType>();
        for (final ContentType contentType: contentTypes) {
            map.put(contentType.getMimeType(), contentType);
        }
        CONTENT_TYPE_MAP = Collections.unmodifiableMap(map);
    }

    // defaults
    public static final ContentType DEFAULT_TEXT = TEXT_PLAIN;
    public static final ContentType DEFAULT_BINARY = APPLICATION_OCTET_STREAM;

    private final String mimeType;
    private final Charset charset;

    ContentType(final String mimeType, final Charset charset) {
        this.mimeType = mimeType;
        this.charset = charset;
    }

    public String getMimeType() {
        return this.mimeType;
    }

    public Charset getCharset() {
        return this.charset;
    }


    private static boolean valid(final String s) {
        for (int i = 0; i < s.length(); i++) {
            final char ch = s.charAt(i);
            if (ch == '"' || ch == ',' || ch == ';') {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a new instance of {@link ContentType}.
     *
     * @param mimeType MIME type. It may not be {@code null} or empty. It may not contain
     *        characters {@code <">, <;>, <,>} reserved by the HTTP specification.
     * @param charset charset.
     * @return content type
     */
    public static ContentType create(final String mimeType, final Charset charset) {
        return new ContentType(mimeType, charset);
    }

    /**
     * Creates a new instance of {@link ContentType} without a charset.
     *
     * @param mimeType MIME type. It may not be {@code null} or empty. It may not contain
     *        characters {@code <">, <;>, <,>} reserved by the HTTP specification.
     * @return content type
     */
    public static ContentType create(final String mimeType) {
        return create(mimeType, (Charset) null);
    }



    /**
     * Returns {@code Content-Type} for the given MIME type.
     *
     * @param mimeType MIME type
     * @return content type or {@code null} if not known.
     *
     * @since 4.5
     */
    public static ContentType getByMimeType(final String mimeType) {
        if (mimeType == null) {
            return null;
        }
        return CONTENT_TYPE_MAP.get(mimeType);
    }

    /**
     * Creates a new instance with this MIME type and the given Charset.
     *
     * @param charset charset
     * @return a new instance with this MIME type and the given Charset.
     * @since 4.3
     */
    public ContentType withCharset(final Charset charset) {
        return create(this.getMimeType(), charset);
    }



}
