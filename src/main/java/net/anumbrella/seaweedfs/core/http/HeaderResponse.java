package net.anumbrella.seaweedfs.core.http;

//import org.apache.http.Header;

import java.nio.charset.Charset;
import java.util.Arrays;

import okhttp3.internal.http2.Header;


public class HeaderResponse {
    private Header[] headers;
    private int httpResponseStatusCode;

    public HeaderResponse(Header[] headers, int httpResponseStatusCode) {
        this.httpResponseStatusCode = httpResponseStatusCode;
        if (headers == null){
            return;
        }
        this.headers = headers;
    }

    public Header[] getHeaders() {
        return headers;
    }

    public int getHttpResponseStatusCode() {
        return httpResponseStatusCode;
    }

    public Header getLastHeader(String name) {
        for (int index = headers.length - 1; index > -1; index--) {
            if (headers[index].name.string(Charset.forName("utf-8")).equals(name)){
                return headers[index];
            }
        }
        return null;
    }

    public Header getFirstHeader(String name) {
        for (int index = 0; index < headers.length; index++) {
            if (headers[index].name.string(Charset.forName("utf-8")).equals(name)){
                return headers[index];
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "HeaderResponse{" +
                "headers=" + Arrays.toString(headers) +
                ", httpResponseStatusCode=" + httpResponseStatusCode +
                '}';
    }
}
