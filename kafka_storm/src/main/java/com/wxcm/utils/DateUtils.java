/** 
 * Project Name:
 * File Name:DateUtils.java 
 * Package Name:cn.themis.utils 
 * Date:2015年8月26日下午5:28:20 
 * Copyright (c) 2015, wangchao All Rights Reserved. 
 * 
*/

package com.wxcm.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

/**
 * ClassName:DateUtils <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2015年8月26日 下午5:28:20 <br/>
 * 
 * @author wangchao
 * @version
 * @since JDK 1.6
 * @see
 */
public class DateUtils {
	private static SimpleDateFormat dayParse = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat monthParse = new SimpleDateFormat("yyyyMM");
	private static SimpleDateFormat seconParse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat daysParse = new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat yearParse = new SimpleDateFormat("yyyy");
	private static SimpleDateFormat simpFormat = null;
	
	public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

	private static String millits = null;
	private static int mill = 0;
	
	
	private static String startTime = null;
	private static String endTime = null;
	private static int startSeconds = 0;
	private static int endSeconds = 0;

	/**
	 * 
	 * @description 按照默认的format 转换
	 * @param str
	 * @return
	 * @throws ParseException
	 */
	public static Date convertToDate(String str) throws ParseException {
		return convertToDate(str, DEFAULT_FORMAT);
	}

	/**
	 * 
	 * @description 按照指定format 转换时间
	 * @param str
	 * @param format
	 * @return
	 * @throws ParseException
	 */
	public static Date convertToDate(String str, String format) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
		return simpleDateFormat.parse(str);
	}

	/*
	 * 获取当前日期，时间类型的
	 */
	public static Date currentDate() {
		Long date = System.currentTimeMillis();
		Date current = new Date(date);
		return current;
	}

	/*
	 * 将日期类型的时间字符串，转换成Date类型
	 */
	public static Date convertStrToDate(String day) throws ParseException {
		// day = "2015-09-06 23:23:12";
		Date parse = dayParse.parse(day);
		return parse;

	}

	/*
	 * 计算两个日期之间的差值，相差多少个小时，日期格式都为yyyy-MM-dd 第一个时间为yyyy-MM-dd HH:mm:ss
	 * 第二个时间为yyyy-MM-dd
	 */

	public static int diffHour(String day1, String day2) throws ParseException {
		Date parse = seconParse.parse(day1);
		Date parse2 = dayParse.parse(day2);
		long time = parse.getTime();
		long time2 = parse2.getTime();
		long diffTimes = time2 - time;
		int hours = (int) (diffTimes / 1000 / 60 / 60);
		return hours;
	}

	/*
	 * 穿入一个日期格式的字符串和格式化的规则，返回这个时间的秒值
	 */
	public static int dateToSeco(String date,String format) throws ParseException {
		simpFormat = new SimpleDateFormat(format);
		Date parse = simpFormat.parse(date);
		int startSeconds = (int) (parse.getTime()/1000);
		return startSeconds;
	}

	/*
	 * 将日期字符串，yyyy-MM-dd HH:mm:ss转换成yyyy-MM-dd 这种日期格式的
	 */
	public static String secoToDate(String date) throws ParseException {
		Date parse = seconParse.parse(date);
		String format = dayParse.format(parse);
		return format;
	}

	/*
	 * 将毫秒值转换成标准化日期yyyy-MM-dd HH:mm:ss这种格式
	 */

	public static String longToDate(long mills) {
		Date date = new Date(mills);
		String format = seconParse.format(date);
		return format;
	}

	// 获取当前时间yyyyMMdd
	public static String toDate() {
		Date date = new Date();
		return daysParse.format(date);
	}

	public static Date getSecosDate() {
		Date date = new Date();
		return date;

	}

	public static String currentTime() {
		Date date = new Date();
		return seconParse.format(date);

	}

	/*
	 * 将Date类型的日期转换成标准格式化的字符串
	 */
	public static String dateToStr(Date date) {
		return dateToStr(date, "yyyy-MM-dd");
	}

	/**
	 * 
	 * @description 将时间转换成指定格式的字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String dateToStr(Date date, String format) {
		return new SimpleDateFormat(format).format(date);
	}

	/*
	 * 比较两个日期时间，返回日期较大的那个时间
	 */
	public static Date compareMaxDate(Date date1, Date date2) {
		long time1 = date1.getTime();
		long time2 = date2.getTime();
		if (time1 > time2) {
			return date1;
		} else {
			return date2;
		}
	}

	/*
	 * 求两个时间的差值，相差多少个小时
	 */
	public static int dateSubduction(Date date1, Date date2) {
		long time1 = date1.getTime();
		long time2 = date2.getTime();
		long subductionTime = 0;
		subductionTime = time1 - time2;
		// 将获取的时间差值，转换成小时数
		int hours = (int) (subductionTime / 1000 / 3600);
		return hours;
	}

	public static Date StrToDateSeco(String str) {
		Date parse = null;
		try {
			parse = seconParse.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return parse;
	}

	/**
	 * 开始时间为一周前 当天的0点时间
	 * 
	 * 跟据参数值获取时间：比如 dateSelect=1 获取距离当前时间前一周的时间 dateSelect=2 获取距离当前时间前三周的时间
	 * 0：默认是当天 1：最近一周 2：最近一月 3：最近三月 4：最近半年 5：近一年 6：2年前
	 * 
	 * @param dateSelect
	 * @return
	 */
	public static Date getSelectDateStart(String dateSelect) {
		Date date = null;
		String timeMis = "";
		Calendar calendar = Calendar.getInstance();
		try {
			if (StringUtils.isNotBlank(dateSelect)) {
				if ("0".equals(dateSelect)) {

				} else if ("1".equals(dateSelect)) {
					calendar.add(Calendar.DAY_OF_WEEK, -6);
				} else if ("2".equals(dateSelect)) {
					calendar.add(Calendar.MONTH, -1);
				} else if ("3".equals(dateSelect)) {
					calendar.add(Calendar.MONTH, -3);
				} else if ("4".equals(dateSelect)) {
					calendar.add(Calendar.MONTH, -6);
				} else if ("5".equals(dateSelect)) {
					calendar.add(Calendar.YEAR, -1);
				} else if ("6".equals(dateSelect)) {
					calendar.add(Calendar.YEAR, -2);
				}
			}

			timeMis = DateUtils.longToDate(calendar.getTimeInMillis());
			date = DateUtils.convertStrToDate(timeMis.substring(0, timeMis.indexOf(" ")) + " 00:00:00");

		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 与上面对应，获取时间区间结尾时间
	 * 
	 * @param dateSelect
	 * @return
	 */
	public static Date getSelectDateEnd(String dateSelect) {
		Calendar calendar = Calendar.getInstance();
		if (StringUtils.isBlank(dateSelect)) {
			return new Date();
		}
		if ("1".equals(dateSelect)) {
			calendar.add(Calendar.DAY_OF_WEEK, -6);
		} else if ("2".equals(dateSelect)) {
			calendar.add(Calendar.MONTH, -1);
		} else if ("3".equals(dateSelect)) {
			calendar.add(Calendar.MONTH, -3);
		} else if ("4".equals(dateSelect)) {
			calendar.add(Calendar.MONTH, -6);
		} else if ("5".equals(dateSelect)) {
			calendar.add(Calendar.YEAR, -1);
		} else if ("6".equals(dateSelect)) {
			calendar.add(Calendar.YEAR, -2);
		} else if ("7".equals(dateSelect)) {
			calendar.add(Calendar.YEAR, 1);// 往后延迟一年
		}
		return calendar.getTime();
	}

	public static Date before(Date date, DateType dateType, long time) {
		long result = date.getTime();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		switch (dateType) {
		case SECOND:
			result = result - time * 1000;
			break;
		case MINUTE:
			result = result - time * 1000 * 60;
			break;
		case HOUR:
			result = result - time * 1000 * 60 * 60;
			break;
		case DAY:
			result = result - time * 1000 * 60 * 60 * 24;
			break;
		case WEEK:
			result = result - time * 1000 * 60 * 60 * 24 * 7;
			break;
		case MONTH:
			calendar.add(Calendar.MONTH, -(int) time);
			result = calendar.getTimeInMillis();
			break;
		case YEAR:
			calendar.add(Calendar.YEAR, -(int) time);
			result = calendar.getTimeInMillis();
			break;
		default:
			result = result - time;
			break;
		}
		return new Date(result);
	}

	/**
	 * @description 返回某个时间段之后的时间
	 * @param date
	 * @param dateType
	 *            时间类型
	 * @param time
	 *            次数 例如返回 一个小时以后 则参数是 ( date,DateType.HOUR,1)
	 * @return
	 */
	public static Date after(Date date, DateType dateType, long time) {
		long result = date.getTime();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		switch (dateType) {
		case SECOND:
			result = result + time * 1000;
			break;
		case MINUTE:
			result = result + time * 1000 * 60;
			break;
		case HOUR:
			result = result + time * 1000 * 60 * 60;
			break;
		case DAY:
			result = result + time * 1000 * 60 * 60 * 24;
			break;
		case WEEK:
			result = result + time * 1000 * 60 * 60 * 24 * 7;
			break;
		case MONTH:
			calendar.add(Calendar.MONTH, (int) time);
			result = calendar.getTimeInMillis();
			break;
		case YEAR:
			calendar.add(Calendar.YEAR, (int) time);
			result = calendar.getTimeInMillis();
			break;
		default:
			result = result + time;
			break;
		}
		return new Date(result);
	}

	public enum DateType {
		MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR;

	}

	/**
	 * 
	 * @description 获取某个月有多少天
	 * @param date
	 * @return
	 */
	public static int getDayNumOfMonth(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getActualMaximum(Calendar.DATE);
	}

	/**
	 * 
	 * @description 是否是闰年
	 * @param date
	 * @return
	 */
	public static boolean isBissextile(Date date) {
		String format = new SimpleDateFormat("yyyy").format(date);
		return isBissextile(Integer.parseInt(format));
	}

	/**
	 * 
	 * @description 是否是闰年
	 * @param year
	 * @return
	 */
	public static boolean isBissextile(int year) {
		if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @description 当前年有多少天
	 * @param year
	 * @return
	 */
	public static int getDaysOfYear(int year) {
		if (isBissextile(year)) {
			return 366;
		}
		return 365;
	}

	/**
	 * 
	 * @description 当前时间所在年有多少天
	 * @param date
	 * @return
	 */
	public static int getDaysOfYear(Date date) {
		if (isBissextile(date)) {
			return 366;
		}
		return 365;
	}

	/**
	 * @description 获取当前时间的年份
	 * @return
	 */
	public static String currentYear() {
		Date date = new Date();
		String format = yearParse.format(date);
		return format;
	}

	public static Date foreverDate() {
		String dateStr = "9999-01-01 00:00:00";
		Date parse = null;
		try {
			parse = seconParse.parse(dateStr);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * 
	 * @description 获取某个时间的某种类型的起始时间
	 * @param date
	 * @param dateType
	 * @return
	 */
	public static Date getCurrentStart(Date date, DateType dateType) {
		Date result = null;
		switch (dateType) {
		case MILLISECOND:
			result = date;
			break;
		case SECOND:
			long time = date.getTime();
			String strOfTime = String.valueOf(time);
			String substring = strOfTime.substring(0, strOfTime.length() - 3);
			result = new Date(Long.parseLong(substring + "000"));
			break;
		case MINUTE:
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:00");
			String format1 = sdf1.format(date);
			try {
				result = sdf1.parse(format1);
			} catch (ParseException e) {
			}
			break;
		case HOUR:
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:00:00");
			String format2 = sdf2.format(date);
			try {
				result = sdf2.parse(format2);
			} catch (ParseException e) {
			}
			break;
		case DAY:
			SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
			String format3 = sdf3.format(date);
			try {
				result = sdf3.parse(format3);
			} catch (ParseException e) {
			}
			break;
		case WEEK:
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			result = calendar.getTime();
			break;
		case MONTH:
			SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy/MM/00 00:00:00");
			String format4 = sdf4.format(date);
			try {
				result = sdf4.parse(format4);
			} catch (ParseException e) {
			}
			break;
		default:
			SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy/00/00 00:00:00");
			String format5 = sdf5.format(date);
			try {
				result = sdf5.parse(format5);
			} catch (ParseException e) {
			}
			break;
		}
		return result;
	}

	/**
	 * 
	 * @description 获取某个时间段的 (某种格式的)末尾时间
	 * @param date
	 * @param dateType
	 * @return
	 */
	public static Date getCurrentEnd(Date date, DateType dateType) {
		SimpleDateFormat defaultFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date result = null;
		switch (dateType) {
		case MILLISECOND:
			result = date;
			break;
		case SECOND:
			long time = date.getTime();
			String strOfTime = String.valueOf(time);
			String substring = strOfTime.substring(0, strOfTime.length() - 3);
			result = new Date(Long.parseLong(substring + "999"));
			break;
		case MINUTE:
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:59");
			String format1 = sdf1.format(date);
			try {
				result = defaultFormat.parse(format1);
			} catch (ParseException e) {
			}
			break;
		case HOUR:
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:59:59");
			String format2 = sdf2.format(date);
			try {
				result = defaultFormat.parse(format2);
			} catch (ParseException e) {
			}
			break;
		case DAY:
			SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd 23:59:59");
			String format3 = sdf3.format(date);
			try {
				result = defaultFormat.parse(format3);
			} catch (ParseException e) {
			}
			break;
		case WEEK:
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			// calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.set(Calendar.HOUR_OF_DAY, 23);
			calendar.set(Calendar.MINUTE, 59);
			calendar.set(Calendar.SECOND, 59);
			calendar.add(Calendar.DATE, 7);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			result = calendar.getTime();
			break;
		case MONTH:
			Calendar calendar1 = Calendar.getInstance();
			calendar1.setTime(date);
			calendar1.set(Calendar.DAY_OF_MONTH, 1);
			calendar1.set(Calendar.HOUR_OF_DAY, 23);
			calendar1.set(Calendar.MINUTE, 59);
			calendar1.set(Calendar.SECOND, 59);
			calendar1.add(Calendar.MONTH, 1);
			calendar1.add(Calendar.DAY_OF_MONTH, -1);
			result = calendar1.getTime();
			break;
		default:
			SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy/12/31 23:59:59");
			String format5 = sdf5.format(date);
			try {
				result = defaultFormat.parse(format5);
			} catch (ParseException e) {
			}
			break;
		}
		return result;
	}

	public static Date getSpecialDate() {
		String dateStr = "1970-01-01 00:00:00";
		Date parse = null;
		try {
			parse = seconParse.parse(dateStr);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String currentMonth() {
		Date date = new Date();
		String format = monthParse.format(date);
		return format;
	}

	/**
	 * 返回指定日期的月的第一天
	 *
	 * @param year
	 * @param month
	 * @return
	 * @throws ParseException
	 */
	public static Date getFirstDayOfMonth(Date date) throws ParseException {

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), 1);
		return calendar.getTime();
	}

	/**
	 * 返回指定日期的月的最后一天
	 *
	 * @param year
	 * @param month
	 * @return
	 * @throws ParseException
	 */
	public static Date getLastDayOfMonth(Date date) throws ParseException {

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), 1);
		calendar.roll(Calendar.DATE, -1);
		return calendar.getTime();
	}

	/**
	 * 返回指定日期的月的最后一天，还得向后推一个月
	 */
	public static Date getLastDayOfMonthAndOne(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, 1);
		calendar.roll(Calendar.DATE, -1);
		return calendar.getTime();

	}

	/**
	 * 获取月份的最后一天
	 */
	public static int getLastDay(Date date) {

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), 1);
		calendar.roll(Calendar.DATE, -1);
		Date time = calendar.getTime();
		String format = dayParse.format(time);
		String[] split = format.split("-");
		String day = split[2];
		return Integer.parseInt(day);

	}

	/**
	 * 返回指定日期的季的第一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getFirstDayOfQuarter(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return getFirstDayOfQuarter(calendar.get(Calendar.YEAR), getQuarterOfYear(date));
	}

	/**
	 * 通过指定日期，返回该日期所属季度的月份,例如2015-11-06，返回10，例如2015-08-06，返回7
	 */
	public static int getFirstMonthOfQuarter(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		Date firstDayOfQuarter = getFirstDayOfQuarter(calendar.get(Calendar.YEAR), getQuarterOfYear(date));
		int i = calendar.get(Calendar.MONTH);
		return i;
	}

	// 获取到季度之后，还要将这个时间拼接好，年，月，日，拼接成完整的时间格式，然后计算两个时间相差多少个季度

	/**
	 * 通过指定日期，返回该日期所属季度的最后一个月份，例如2015-11-06，返回12
	 */

	public static int getLastMonthOfQuater(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		Date lastDayOfQuarter = getLastDayOfQuarter(calendar.get(Calendar.YEAR), getQuarterOfYear(date));
		int quarter = getQuarterOfYear(date);
		calendar.setTime(lastDayOfQuarter);
		// System.out.println(quarter);
		// System.out.println(lastDayOfQuarter);
		int i = calendar.get(Calendar.MONTH) + 1;
		return i;
	}

	/**
	 * 返回指定日期的季的最后一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getLastDayOfQuarter(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return getLastDayOfQuarter(calendar.get(Calendar.YEAR), getQuarterOfYear(date));
	}

	/**
	 * 返回指定日期的上一季的最后一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getLastDayOfLastQuarter(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return getLastDayOfLastQuarter(calendar.get(Calendar.YEAR), getQuarterOfYear(date));
	}

	/**
	 * 返回指定年季的季的第一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getFirstDayOfQuarter(Integer year, Integer quarter) {
		Calendar calendar = Calendar.getInstance();
		Integer month = new Integer(0);
		if (quarter == 1) {
			month = 1 - 1;
		} else if (quarter == 2) {
			month = 4 - 1;
		} else if (quarter == 3) {
			month = 7 - 1;
		} else if (quarter == 4) {
			month = 10 - 1;
		} else {
			month = calendar.get(Calendar.MONTH);
		}
		return getFirstDayOfMonth(year, month);
	}

	/**
	 * 返回指定年月的月的第一天
	 *
	 * @param year
	 * @param month
	 * @return
	 */
	public static Date getFirstDayOfMonth(Integer year, Integer month) {
		Calendar calendar = Calendar.getInstance();
		if (year == null) {
			year = calendar.get(Calendar.YEAR);
		}
		if (month == null) {
			month = calendar.get(Calendar.MONTH);
		}
		calendar.set(year, month, 1);
		return calendar.getTime();
	}

	/**
	 * 返回指定日期的季度
	 *
	 * @param date
	 * @return
	 */
	public static int getQuarterOfYear(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MONTH) / 3 + 1;
	}

	public static int getFirstMonthOfQuarters(Date date) {
		int quarterOfYear = getQuarterOfYear(date);
		if (1 == quarterOfYear) {
			return 1;
		} else if (2 == quarterOfYear) {
			return 4;
		} else if (3 == quarterOfYear) {
			return 7;
		} else if (4 == quarterOfYear) {
			return 10;
		}
		return 1;
	}

	/**
	 * 返回指定年季的上一季的最后一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getLastDayOfLastQuarter(Integer year, Integer quarter) {
		Calendar calendar = Calendar.getInstance();
		Integer month = new Integer(0);
		if (quarter == 1) {
			month = 12 - 1;
		} else if (quarter == 2) {
			month = 3 - 1;
		} else if (quarter == 3) {
			month = 6 - 1;
		} else if (quarter == 4) {
			month = 9 - 1;
		} else {
			month = calendar.get(Calendar.MONTH);
		}
		return getLastDayOfMonth(year, month);
	}

	/**
	 * 返回指定年月的月的最后一天
	 *
	 * @param year
	 * @param month
	 * @return
	 */
	public static Date getLastDayOfMonth(Integer year, Integer month) {
		Calendar calendar = Calendar.getInstance();
		if (year == null) {
			year = calendar.get(Calendar.YEAR);
		}
		if (month == null) {
			month = calendar.get(Calendar.MONTH);
		}
		calendar.set(year, month, 1);
		calendar.roll(Calendar.DATE, -1);
		return calendar.getTime();
	}

	/**
	 * 返回指定年季的季的最后一天
	 *
	 * @param year
	 * @param quarter
	 * @return
	 */
	public static Date getLastDayOfQuarter(Integer year, Integer quarter) {
		Calendar calendar = Calendar.getInstance();
		Integer month = new Integer(0);
		if (quarter == 1) {
			month = 3 - 1;
		} else if (quarter == 2) {
			month = 6 - 1;
		} else if (quarter == 3) {
			month = 9 - 1;
		} else if (quarter == 4) {
			month = 12 - 1;
		} else {
			month = calendar.get(Calendar.MONTH);
		}
		return getLastDayOfMonth(year, month);
	}

	/**
	 * 获取指定日期所属半年的开始时间
	 */
	public static Date getHalfYearBeginDate(Date date) {

		try {
			String strDate = "";
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String format2 = format.format(date);
			String[] split = format2.split("-");
			String year = split[0];
			String month = split[1];
			int parseInt = Integer.parseInt(month);
			if (parseInt <= 6) {
				strDate = year + "-" + "01" + "-" + "01";

			} else {
				strDate = year + "-" + "07" + "-" + "01";
			}
			Date parse;
			parse = format.parse(strDate);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * 获取指定日期所属半年的结束时间
	 */
	public static Date getHalfYearEndDate(Date date) {

		try {
			String strDate = "";
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String format2 = format.format(date);
			String[] split = format2.split("-");
			String year = split[0];
			String month = split[1];
			int parseInt = Integer.parseInt(month);
			if (parseInt <= 6) {
				strDate = year + "-" + "06" + "-" + "30";

			} else {
				strDate = year + "-" + "12" + "-" + "31";
			}
			Date parse = format.parse(strDate);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/*
	 * 获取指定日期所属年份的第一天
	 */
	public static Date getCurrentYearBegDate(Date date) {

		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = format.format(date);
			String[] split = strDate.split("-");
			String year = split[0];
			String beginDate = year + "-" + "01" + "-" + "01";
			Date parse = format.parse(beginDate);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/*
	 * 获取指定日期所属年份的最后一天
	 */
	public static Date getCurrentYearEndDate(Date date) {

		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String strDate = format.format(date);
			String[] split = strDate.split("-");
			String year = split[0];
			String yearEndDate = year + "-" + "12" + "-" + "31";
			Date parse = format.parse(yearEndDate);
			return parse;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * 得到某年某周的第一天
	 *
	 * @param year
	 * @param week
	 * @return
	 */
	public static Date getFirstDayOfWeek(int year, int week) {
		week = week - 1;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, Calendar.JANUARY);
		calendar.set(Calendar.DATE, 1);

		Calendar cal = (Calendar) calendar.clone();
		cal.add(Calendar.DATE, week * 7);

		return getFirstDayOfWeek(cal.getTime());
	}

	/**
	 * 得到某年某周的最后一天
	 *
	 * @param year
	 * @param week
	 * @return
	 */
	public static Date getLastDayOfWeek(int year, int week) {
		week = week - 1;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, Calendar.JANUARY);
		calendar.set(Calendar.DATE, 1);
		Calendar cal = (Calendar) calendar.clone();
		cal.add(Calendar.DATE, week * 7);

		return getLastDayOfWeek(cal.getTime());
	}

	/**
	 * 取得当前日期所在周的第一天
	 *
	 * @param date
	 * @return
	 */
	public static Date getFirstDayOfWeek(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setFirstDayOfWeek(Calendar.SUNDAY);
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek()); // Sunday
		return calendar.getTime();
	}

	/**
	 * 取得当前日期所在周的最后一天
	 *
	 * @param date
	 * @return
	 */
	public static Date getLastDayOfWeek(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setFirstDayOfWeek(Calendar.SUNDAY);
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek() + 6); // Saturday
		return calendar.getTime();
	}

	/**
	 * 取得当前日期所在周的前一周最后一天
	 *
	 * @param date
	 * @return
	 */
	public static Date getLastDayOfLastWeek(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return getLastDayOfWeek(calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR) - 1);
	}

	/**
	 * 获取年份
	 */
	public static int getCurrentYear(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.setTime(date);
		int i = calendar.get(Calendar.YEAR);

		return i;
	}

	/**
	 * 获取月份
	 */
	public static int getCurrentMonth(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int i = calendar.get(Calendar.MONTH) + 1;
		return i;

	}

	/**
	 * 获取日期
	 */
	public static int getCurrentDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int i = calendar.get(Calendar.DATE);
		return i;
	}

	/**
	 * 获取两个日期相差多少天
	 */
	public static int diffDay(Date date1, Date date2) {
		Calendar aCalendar = Calendar.getInstance();
		aCalendar.setTime(date1);
		int day1 = aCalendar.get(Calendar.DAY_OF_YEAR);
		Calendar bCalendar = Calendar.getInstance();
		bCalendar.setTime(date2);
		int day2 = bCalendar.get(Calendar.DAY_OF_YEAR);
		return day2 - day1;
	}

	/**
	 * 计算两个时间相差多少个月
	 */
	public static int countMonths(String date1, String date2, String pattern) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		Calendar c1 = Calendar.getInstance();
		Calendar c2 = Calendar.getInstance();

		c1.setTime(sdf.parse(date1));
		c2.setTime(sdf.parse(date2));

		int year = c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR);

		// 开始日期若小月结束日期
		if (year < 0) {
			year = -year;
			return year * 12 + c1.get(Calendar.MONTH) - c2.get(Calendar.MONTH);
		}

		return year * 12 + c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH);
	}

	/**
	 * 
	 * @Title: getBelongHalfYear @Description:
	 *         查询所属半年时间，如果是2015上半年，返回2015A，如果是2015下半年，返回2015B @param @param
	 *         date @param @return 设定文件 @return String 返回类型 @throws
	 */
	public static String getBelongHalfYear(Date date) {
		Calendar c1 = Calendar.getInstance();
		c1.setTime(date);
		int year = c1.get(Calendar.YEAR);
		int month = c1.get(Calendar.MONTH) + 1;
		System.out.println(month);
		String halfYear = year + "A";
		if (month <= 6) {
			// 上半年
			halfYear = year + "A";
		} else {
			// 下半年
			halfYear = year + "B";
		}
		return halfYear;
	}

	/**
	 * 获取当天0点时间
	 * 
	 * @return
	 */
	public static Date getCurrentZoreTime(Date startTime) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String strDate = format.format(startTime) + " 00:00:00";
		Date current = null;
		try {
			current = seconParse.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return current;
	}

	/**
	 * @throws ParseException
	 * 
	 * @Title: generateCalcEndDate @Description:
	 *         通过尽调周期类型，和截止日期时间来计算出委托任务最终的计算结束时间 @param @param
	 *         specialType @param @param taskEndTime @param @return 设定文件 @return
	 *         Date 返回类型 @throws
	 */
	public static Date generateCalcEndDate(String specialType, Date taskEndTime) throws ParseException {
		Date date = taskEndTime;
		if ("1".equals(specialType)) {
			// 按月尽调
			date = DateUtils.getLastDayOfMonth(taskEndTime);
		} else if ("2".equals(specialType)) {
			// 按季度尽调
			date = DateUtils.getLastDayOfQuarter(taskEndTime);
		} else if ("3".equals(specialType)) {
			// 按半年尽调
			date = DateUtils.getHalfYearEndDate(taskEndTime);

		} else if ("4".equals(specialType)) {
			// 按年尽调
			date = DateUtils.getCurrentYearEndDate(taskEndTime);
		}

		return date;
	}

	public static Long currentTimeMills() {
		Date date = new Date();

		return date.getTime();
	}



	public static int getMinute(Date date) {
		millits = DateUtils.dateToStr(date, "mm");
		mill = Integer.parseInt(millits);
		if (mill >= 0 && mill <= 10) {
			mill = 10;
		}

		if (mill >= 11 && mill <= 20) {
			mill = 20;
		}

		if (mill >= 21 && mill <= 30) {
			mill = 30;
		}

		if (mill >= 31 && mill <= 40) {
			mill = 40;
		}

		if (mill >= 41 && mill <= 50) {
			mill = 50;
		}

		if (mill >= 51 && mill < 60) {
			mill = 60;
		}
		return mill;
	}

	public static void main(String[] args) throws ParseException {
		int currentTimeMillits = DateUtils.getMinute(new Date());
		System.out.println(currentTimeMillits);
	}
	
	
	
	public static int getTodayStartSeconds() throws ParseException {
		startTime = DateUtils.dateToStr(new Date(), "yyyy-MM-dd");
		startTime = startTime+" 00:00:00";
		startSeconds = DateUtils.dateToSeco(startTime,"yyy-MM-dd HH:mm:ss");
		return startSeconds;
	}

	public static int getTodayEndSeconds() throws ParseException {
		endTime = DateUtils.dateToStr(new Date(), "yyyy-MM-dd");
		endTime = endTime+" 23:59:59";
		endSeconds = DateUtils.dateToSeco(endTime,"yyy-MM-dd HH:mm:ss");
		return endSeconds;
	}

}