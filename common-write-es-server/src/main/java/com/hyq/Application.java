package com.hyq;

import com.alibaba.fastjson.JSON;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 使用过程中，有任何问题，请联系侠梦
 *
 * 【侠梦的开发笔记】微信公众号，回复【加群】
 */
@CrossOrigin(origins = "*")
@SpringBootApplication
@RestController
@RequestMapping("/bulk")
public class Application {

	//5.X版本 带type
	public static String  META_DATA_V1 = "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n";

	//7.1.X版本不带type
	public static String  META_DATA_V2 = "{ \"index\" : { \"_index\" : \"%s\"} }%n";


	public static String ES_URL = "http://你的es ip:9200";

	public static String USERNAME = "elastic";
	public static String PASSWORD = "d8ZnwXDUlmjZQDBx19je";


	private static RestTemplate restTemplate;
	static {

		UsernamePasswordCredentials cred = new UsernamePasswordCredentials(USERNAME, PASSWORD);
		BasicCredentialsProvider cp = new BasicCredentialsProvider();
		cp.setCredentials(AuthScope.ANY, cred);
		CloseableHttpClient client = HttpClientBuilder.create()
				.setDefaultCredentialsProvider(cp)
				.build();
		ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(client);

//		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
//		factory.setConnectTimeout(30_300);
//		factory.setReadTimeout(30_300);
		restTemplate= new RestTemplate(factory);
	}



public  static void main(String[] args) {
	SpringApplication.run(Application.class, args);

	}




	/**
	 * 请求格式为JSON数组
	 *
	 * [
	 * {
	 *     "msg":"这是简单的消息",
	 *     "os":"Windows7",
	 *     "line":"36",
	 *     "time":15698987666667,
	 *     "vid":"ffdgdfgdfg",
	 *     "column":"4",
	 *     "language":"zh_CN",
	 *     "requestIP":"10.3.0.51"
	 * },
	 * {
	 *     "msg":"这是简单的消息",
	 *     "os":"Windows7",
	 *     "line":"36",
	 *     "time":15698987666667,
	 *     "vid":"ffdgdfgdfg",
	 *     "column":"4",
	 *     "language":"zh_CN",
	 *     "requestIP":"10.3.0.51"
	 * }
	 *
	 * ]
	 * 高版本写入
	 * @param index
	 * @param bulkDatas
	 * @return
	 */
	@RequestMapping("/v2/{index}")
	public String bulkV2(@PathVariable("index") String index, @RequestBody  List<Map<String,Object>> bulkDatas){

		if(bulkDatas == null || bulkDatas.size() == 0){
			return "empty body";
		}
		return bulkToEs(index,"",META_DATA_V2, bulkDatas);
	}




	/**
	 * 低版本写入
	 * @param index
	 * @param bulkDatas
	 * @return
	 */
	@RequestMapping("/v1/{index}/{type}")
	public String bulkV1(@PathVariable("index") String index,@PathVariable("type") String type, @RequestBody  List<Map<String,Object>> bulkDatas){

		if(bulkDatas == null || bulkDatas.size() == 0){
			return "empty body";
		}
		return bulkToEs(index,type,META_DATA_V1, bulkDatas);
	}



	/**
	 * 批量插入ES
	 * @param index 索引
	 * @param metadata
	 * @param bulkDatas
	 * @return
	 */
	public String bulkToEs( String index,String type ,String metadata , @RequestBody List<Map<String, Object>> bulkDatas) {
		StringBuilder bulkRequestBody = new StringBuilder();
		for (Map<String,Object> bulkItem : bulkDatas) {

			String  actionMetaData;
			if(StringUtils.isEmpty(type)){
				  actionMetaData = String.format(metadata, index);
			}else{
				  actionMetaData = String.format(metadata, index,type);
			}
			bulkRequestBody.append(actionMetaData);
			bulkRequestBody.append(JSON.toJSONString(bulkItem));
			bulkRequestBody.append("\n");
		}
		String body = bulkRequestBody.toString();
//		log.warn(body.getBytes().length +"");

		String indexPath = String.join("/", index, "_bulk");
		String endpoint = "/" + indexPath;

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
		HttpEntity<String> request = new HttpEntity<>(body, headers);


		ResponseEntity<String> exchange = restTemplate.exchange(ES_URL + endpoint, HttpMethod.POST, request, String.class);
		return Optional.ofNullable(exchange).map(ResponseEntity::toString).orElse("empty result");
	}


}
