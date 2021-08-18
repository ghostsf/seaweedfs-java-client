package net.anumbrella.seaweedfs.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.anumbrella.seaweedfs.core.http.JsonResponse;
import net.anumbrella.seaweedfs.core.http.StreamResponse;
import net.anumbrella.seaweedfs.exception.SeaweedfsFileNotFoundException;
import net.anumbrella.seaweedfs.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;


public class FilerWrapper {
    private Connection connection;
    private ObjectMapper objectMapper = new ObjectMapper();

    public FilerWrapper(Connection connection) {
        this.connection = connection;
    }


    public long uploadFile(String url, String fileName, File file, ContentType contentType) throws IOException {
        RequestBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("upload", fileName,
                        RequestBody.create(MediaType.parse(contentType.getMimeType()), file))
                .build();
        Request request = new Request.Builder()
                .url(url)
                .header("Accept-Language", "zh-cn")
                .post(requestBody)
                .build();


        JsonResponse jsonResponse = connection.fetchJsonResultByRequest(request);
        if (jsonResponse == null) {
            jsonResponse = new JsonResponse("{\"name\":\"" + fileName + "\",\"size\":0}", 200);
        }
        Utils.convertResponseStatusToException(jsonResponse.statusCode, url, false, false, false, false);
        return (Integer) objectMapper.readValue(jsonResponse.json, Map.class).get("size");
    }

    public StreamResponse getFileStream(String url) throws IOException {
        Request request = new Request.Builder().url(url).get().build();
        StreamResponse cache = connection.fetchStreamCacheByRequest(request);
        Utils.convertResponseStatusToException(cache.getHttpResponseStatusCode(), url, false, false, false, false);
        return cache;
    }

    public void deleteFile(String url) throws IOException {
//        HttpDelete httpDelete = new HttpDelete(url);
        Request httpDelete = new Request.Builder().url(url).delete().build();
        JsonResponse jsonResponse = connection.fetchJsonResultByRequest(httpDelete);
        if (jsonResponse != null) {
            Utils.convertResponseStatusToException(connection.fetchJsonResultByRequest(httpDelete).statusCode,
                    url, false, false, false, false);
        } else {
            //SeaweedFS用filer删除文件时没有返回值，只有204状态
            Utils.convertResponseStatusToException(204, url, false, false, false, false);
        }
    }

    public boolean checkFileExist(String url) throws IOException {
//        HttpHead request = new HttpHead(url);
        Request request = new Request.Builder().url(url).head().build();
        final int statusCode = connection.fetchStatusCodeByRequest(request);
        try {
            Utils.convertResponseStatusToException(statusCode, url, false, true, false, false);
            return true;
        } catch (SeaweedfsFileNotFoundException e) {
            return false;
        }
    }
}
