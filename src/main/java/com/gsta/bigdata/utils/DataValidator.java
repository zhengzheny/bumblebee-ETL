package com.gsta.bigdata.utils;

import org.apache.commons.validator.GenericValidator;
/**
 * @author tianxq
 *
 */
public class DataValidator {
	public static boolean isByte(String value){
		return GenericValidator.isByte(value);
	}
	
	public static boolean isCreditCard(String value){
		return GenericValidator.isCreditCard(value);
	}
	
	public static boolean isDouble(String value){
		return GenericValidator.isDouble(value);
	}
	
	public static boolean isEmail(String value){
		return GenericValidator.isEmail(value);
	}
	
	public static boolean isFloat(String value){
		return GenericValidator.isFloat(value);
	}
	
	public static boolean isInt(String value){
		return GenericValidator.isInt(value);
	}
	
	public static boolean isLong(String value){
		return GenericValidator.isLong(value);
	}
	
	public static boolean isShort(String value){
		return GenericValidator.isShort(value);
	}
	
	public static boolean isString(String value){
		return true;
	}
	
	public static boolean isIdCard(String value){
		return IDCardUtil.isIDCard(value);
	}
}
