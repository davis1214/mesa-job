package com.di.mesa.metric.util;

import static java.lang.System.out;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Created by davi on 17/9/21.
 */
public class GsonUtil {

	private static final Gson GSON = new Gson();

	private static final JsonParser JSON_PARSER = new JsonParser();

	private static final Type MAP_TYPE = new TypeToken<Map<String, ?>>() {
	}.getType();

	private static final Type JSON_OBJECT_TYPE = new TypeToken<List<JsonObject>>() {
	}.getType();

	/**
	 * 将对象转换成Json字符串
	 *
	 * @param src
	 * @return
	 */
	public static String toJson(Object src) {
		return GSON.toJson(src);
	}

	/**
	 * 将Json转换成对象
	 *
	 * @param json
	 * @param classOfT
	 * @return
	 */
	public static <T> T toEntity(String json, Class<T> classOfT) {
		return GSON.fromJson(json, classOfT);
	}

	/**
	 * 将Json转化成Map
	 *
	 * @param json
	 * @return
	 */
	public static Map<String, ?> toMap(String json) {
		return GSON.fromJson(json, MAP_TYPE);
	}

	/**
	 * 将Json字符串转化成List
	 *
	 * @param json
	 * @param typeOfT
	 * @return
	 */
	public static <T> List<T> toList(String json, Class<T> typeOfT) {
		List<JsonObject> jsonObjectList = GSON.fromJson(json, JSON_OBJECT_TYPE);
		List<T> list = new ArrayList<>();
		for (JsonObject jsonObject : jsonObjectList) {
			list.add(toEntity(jsonObject.toString(), typeOfT));
		}
		return list;
	}


	public static List<Map<String, ?>> toList(String json) {
		return GSON.fromJson(json, new TypeToken<List<Map<String, ?>>>() {
		}.getType());
	}

	/**
	 * Json字符串转JsonObject
	 *
	 * @param json
	 * @return
	 */
	public static JsonObject toJsonObject(String json) {
		return JSON_PARSER.parse(json).getAsJsonObject();
	}

	/**
	 * 将JsonObject转换成Json字符串
	 *
	 * @param
	 * @return
	 */
	public static String toJson(JsonObject jsonObject) {
		return jsonObject.toString();
	}

	public static void main(String[] args) {
		String a = "{\\\"messageType\\\":1,\\\"orderChangeToStatus\\\":0,\\\"orderInfoDO\\\":{\\\"add_time\\\":\\\"2017-07-12 04:01:44\\\",\\\"attributes\\\":{\\\"xl_game_id\\\":\\\"hainan\\\",\\\"source\\\":\\\"DETAIL\\\",\\\"wpartner_biz_id\\\":\\\"1038\\\",\\\"xl_fangka_id\\\":\\\"10001\\\",\\\"sup_price\\\":\\\"1776\\\",\\\"xl_user_account\\\":\\\"492166\\\",\\\"cnl\\\":\\\"fx\\\",\\\"v_a_id\\\":\\\"h5\\\"},\\\"buyer_id\\\":\\\"1228899494\\\",\\\"extend\\\":\\\",,h5!h5!NA!NA!1499803235288!NA,,,,,,,,,,,,,,,,,xl_game_id:hainan;source:DETAIL;wpartner_biz_id:1038;xl_fangka_id:10001;sup_price:1776;xl_user_account:492166;cnl:fx;v_a_id:h5;,,,,,,,,,,\\\",\\\"f_seller_id\\\":1117787090,\\\"flag\\\":3,\\\"flag_bin\\\":1060864,\\\"fx_fee\\\":12.0,\\\"id\\\":800064836749902,\\\"orderId\\\":800064836749902,\\\"orderStatus\\\":0,\\\"order_source\\\":401,\\\"order_status_des\\\":\\\"已关闭\\\",\\\"order_status_des_int\\\":0,\\\"refund_flag\\\":false,\\\"seller_id\\\":356895061,\\\"status\\\":60,\\\"sub_orders\\\":[{\\\"item_id\\\":1994950621,\\\"item_sku_id\\\":5890107232,\\\"quantity\\\":1}],\\\"total_price\\\":30.0,\\\"update_time\\\":\\\"2017-07-14 04:01:45\\\"}}";

		String b = "{\"messageType\":1,\"orderChangeToStatus\":3,\"orderInfoDO\":{\"add_time\":\"2017-07-14 03:57:06\",\"attributes\":{},\"buyer_id\":\"1138431412\",\"extend\":\",,c,,,,,,,,,,,,,,,,,,,,,,,,,,,\",\"f_seller_id\":0,\"flag\":3,\"flag_bin\":67108864,\"fx_fee\":0.0,\"id\":800068954450205,\"orderId\":800068954450205,\"orderStatus\":3,\"order_source\":0,\"order_status_des\":\"已发货\",\"order_status_des_int\":3,\"refund_flag\":false,\"seller_id\":1185608641,\"ship_time\":\"2017-07-14 04:00:08\",\"status\":30,\"sub_orders\":[{\"item_id\":2057027159,\"item_sku_id\":0,\"quantity\":1}],\"total_price\":100.0,\"update_time\":\"2017-07-14 04:00:08\"}}";

		out.println(b);

		Map<String, ?> stringMap = toMap(b);

		out.println(stringMap);

		Map<String, ?> orderInfoDO = (Map<String, ?>) stringMap.get("orderInfoDO");

		out.println(orderInfoDO);

		out.println(orderInfoDO.get("sub_orders").getClass().getName());

		List<Map<String, ?>> sub_orders = (List<Map<String, ?>>) orderInfoDO.get("sub_orders");

		for (int i = 0; i < sub_orders.size(); i++) {

			out.println(sub_orders.get(i).toString());
		}

		System.exit(1);

		String orderStatus = orderInfoDO.get("orderStatus").toString();
		String orderStatusDes = (String) orderInfoDO.get("order_status_des");

		String update_time = (String) orderInfoDO.get("update_time");

		String buyerId = (String) orderInfoDO.get("buyer_id");
		Double sellerId = (Double) orderInfoDO.get("seller_id");
		Double orderId = (Double) orderInfoDO.get("orderId");

		out.println("\n");
		out.println(orderStatus + " -> " + orderStatusDes + " -> " + update_time);
		out.println(sellerId + " -> " + buyerId + " -> " + orderId);
		out.println("format :" + sellerId.toString() + " -> " + buyerId + " -> " + orderId);

		orderStatus = orderStatus.substring(0, orderStatus.indexOf("."));
		Map<String, String> jsonMap = Maps.newHashMap();
		jsonMap.put("sellerId", "121321");
		jsonMap.put("buyerId", buyerId);
		jsonMap.put("feedType", String.valueOf(Integer.valueOf(orderStatus) + 1000));
		jsonMap.put("description", "已发货");
		jsonMap.put("from", "tm_order_status");
		jsonMap.put("feedTime", String.valueOf(System.currentTimeMillis() / 1000));

		out.println("\n");
		out.println("----->" + GsonUtil.toJson(jsonMap));

		String aa = "{\"itemRefundDO\":{\"buyer_id\":\"976692970\",\"f_seller_id\":\"\",\"item_list\":[{\"item_id\":\"1909314091\",\"item_sku_id\":\"\"}],\"operate_role\":"
				+ "\"buyer\",\"order_flag_bin\":0,\"order_id\":774597417969857,\"refund_express_fee\":0,\"refund_fee\":9000,\"refund_item_fee\":9000,\"refund_kind\":1,"
				+ "\"refund_no\":4421716,\"refund_operate_status_desc\":\"买家申请退款，待商家处理\",\"refund_operate_status_int\":100,\"refund_status_desc\":\"申请退款\","
				+ "\"refund_status_int\":101,\"seller_id\":314582724,\"update_time\":1499992875000},\"messageType\":2}";

		stringMap = GsonUtil.toMap(aa);

		out.println("======>" + stringMap);
		Map<String, ?> orderInfoDO2 = (Map<String, ?>) stringMap.get("itemRefundDO");

		String buyerId2 = (String) orderInfoDO2.get("buyer_id");
		Double sellerId2 = (Double) orderInfoDO2.get("seller_id");
		Double orderId2 = (Double) orderInfoDO2.get("order_id");

		Object aaaaa = orderInfoDO2.get("order_id");

		out.println(aaaaa.getClass().getCanonicalName());

		// update_time = (String) orderInfoDO2.get("update_time");
		orderStatusDes = (String) orderInfoDO2.get("refund_status_desc");
		Double refund_status_int = (Double) orderInfoDO2.get("refund_status_int");

		out.println(buyerId2 + " -> " + buyerId2 + " -> " + orderId2);
		out.println(orderInfoDO2.get("update_time") + " --> " + orderStatusDes + " --> " + refund_status_int);
		out.println("format :" + sellerId2.toString() + " -> " + buyerId2 + " -> " + orderId2);

		Object fSellerId = orderInfoDO2.get("f_seller_id");
		out.println("fSellerId :" + fSellerId + ", fSellerId ==null :" + (fSellerId.equals("")));

		out.println(getStringValue(fSellerId));

		// Object o = orderInfoDO.get("buyer_id");
		// out.println(getStringValue(o));
		// out.println(getStringValue(orderInfoDO.get("seller_id")));
		// out.println(getStringValue(orderInfoDO.get("orderId")));
		// out.println("orderStatus:"+getStringValue(orderInfoDO.get("orderStatus")).substring(0,orderStatus.indexOf(".")));
		// out.println("order_status_des:"+getStringValue(orderInfoDO.get("order_status_des")));

	}

	//
	public static String getStringValue(Object object) {
		String value = null;
		if (object instanceof String) {
			value = object.toString();
			System.err.println("string type : [" + value + "]");
		} else if (object instanceof Double) {
			BigDecimal d1 = new BigDecimal(object.toString());
			value = d1.toString();
			System.err.println("Double type : [" + value + "]");
		} else {
			System.err.println("ambi type : [" + value + "]");
		}

		return value;
	}

}