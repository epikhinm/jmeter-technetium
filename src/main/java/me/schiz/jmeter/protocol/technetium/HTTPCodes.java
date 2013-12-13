package me.schiz.jmeter.protocol.technetium;

/**
 * Created with IntelliJ IDEA.
 * User: schizophrenia
 * Date: 7/20/13
 * Time: 4:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class HTTPCodes {

    //2xx: Success
    public static String OK_200 = "200";
    public static String CREATED_201 = "201";
    public static String ACCEPTED_202 = "202";


    //4xx: Client errors
    public static String BAD_REQUEST_400 = "400";
    public static String UNAUTHORIZED_401 = "401";
    public static String FORBIDDEN_403 = "403";
    public static String NOT_FOUND_404 = "404";
    public static String METHOD_NOT_ALLOWED_405 = "405";
    public static String REQUEST_TIMEOUT_408 = "408";


    //5xx: Server errors
    public static String INTERNAL_SERVER_ERROR_500 = "500";
    public static String BAD_GATEWAY_502 = "502";
    public static String SERVICE_UNAVAILABLE_503 = "503";

}

