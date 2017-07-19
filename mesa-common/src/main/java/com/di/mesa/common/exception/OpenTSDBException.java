package com.di.mesa.common.exception;

import java.io.IOException;

public class OpenTSDBException extends Exception {

	private static final long serialVersionUID = 1L;
	public int responseCode = 0;
	
	public OpenTSDBException(String message) {
		super(message);
	}
	
	public OpenTSDBException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public OpenTSDBException(int responseCode, String urlString, String dataPoint) throws IOException{
		super("OpenTSDB Error. Code " + responseCode);
		this.responseCode = responseCode;
		System.out.println("Error in OpenTSDB caused by: " + getResponseMessage(responseCode));
		System.out.println("Request URL: " + urlString);
		if(!dataPoint.equals("")){
			System.out.print("Inserting data point :" + dataPoint);
		}
	}
	
	private String getResponseMessage(int responseCode){
		
		String message = "";
		
		switch(responseCode){
		case 400:	message = code400;	break;
		case 404:	message = code404;	break;
		case 405:	message = code405;	break;
		case 406:	message = code406;	break;
		case 408:	message = code408;	break;
		case 413:	message = code413;	break;
		case 500:	message = code500;	break;
		case 501:	message = code501;	break;
		case 503:	message = code503;	break;	
		}
		return "Error Code " + responseCode + ":" + message;
	}
	
	//Messages are from the HTTP api site at http://opentsdb.net/docs/build/html/api_http/index.html
	private String code400 = "Information provided by the API user, via a query string or content data, was in error or missing. This will usually include information in the error body about what parameter caused the issue. Correct the data and try again.";
	private String code404 = "Either the requested annotation does not exist or the requested endpoint or file was not found. This is usually related to the static file endpoint.";
	private String code405 = "The requested verb or method was not allowed. Please see the documentation for the endpoint you are attempting to access.";
	private String code406 = "The request could not generate a response in the format specified. For example, if you ask for a PNG file of the logs endpoing, you will get a 406 response since log entries cannot be converted to a PNG image (easily)";
	private String code408 = "The request has timed out. This may be due to a timeout fetching data from the underlying storage system or other issues.";
	private String code413 = "The results returned from a query may be too large for the server's buffers to handle. This can happen if you request a lot of raw data from OpenTSDB. In such cases break your query up into smaller queries and run each individually";
	private String code500 = "An internal error occured within OpenTSDB. Make sure all of the systems OpenTSDB depends on are accessible and check the bug list for issues";
	private String code501 = "The requested feature has not been implemented yet. This may appear with formatters or when calling methods that depend on plugins";
	private String code503 = "A temporary overload has occurred. Check with other users/applications that are interacting with OpenTSDB and determine if you need to reduce requests or scale your system.";
}