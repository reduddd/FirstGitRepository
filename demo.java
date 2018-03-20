public class demo implements Job,Serializable{
  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
/**
   * 每天1点启动任务
   */
	private static boolean isNext = true;
	private IBIExtraService biExtraService;
	private IClipUsersService clipUsersService;
	private IWeiboDailyStatisticsService 
weiboDailyStatisticsService;
	private static Logger logger = 
LoggerFactory.getLogger(TimedTask.class);


	@SuppressWarnings("unlikely-arg-type")
	@Override
	public void execute(JobExecutionContext arg0) throws 
JobExecutionException {
		if(!isNext) {
			return;
		}
		
logger.info("------------TimedTask-------start-------------");
		isNext = false;
		try {
			SchedulerContext context = 
arg0.getScheduler().getContext();
			ApplicationContext ctx = (ApplicationContext) 
context.get("applicationContextKey");
			this.biExtraService = 
ctx.getBean(BIExtraService.class);
			
this.clipUsersService=ctx.getBean(ClipUsersService.class);
			
this.weiboDailyStatisticsService=ctx.getBean(WeiboDailyStatisticsService.class);

			Calendar cal = Calendar.getInstance();
			CalendarUtils.clearHMS(cal);
			Date nextEndTime = cal.getTime();
			cal.add(Calendar.DATE, -1);
			Date startTime = cal.getTime();

			List<WeiboDailyStatisticsDTO> 
weiboDailyStatisticsDtoList=new ArrayList<WeiboDailyStatisticsDTO>();
			Map<String,Object> contions=new HashMap<String, 
Object>();
			
			Map<String, Object> conditions = new 
HashMap<String, Object>();
			conditions.clear();
			conditions.put("state", 1);
			conditions.put("notUserTypes", 
"'iss','ims','admin','dev','test'");
			List<ClipUsersDTO> clipUsersList = 
this.clipUsersService.getUsersList(conditions);

			conditions.clear();
			conditions.put("state", 1);
			conditions.put("userTypes", "'iss','ims'");
			List<ClipUsersDTO> extraUsersList = 
this.biExtraService.getUsersList(conditions);
			//所有有效用户
			Map<Long, ClipUsersDTO> usersMap = new 
HashMap<Long, ClipUsersDTO>();
			for (ClipUsersDTO dto : clipUsersList) {
				usersMap.put(dto.getId(), dto);
			}
			for (ClipUsersDTO dto : extraUsersList) {
				usersMap.put(dto.getId(), dto);
			}
			
			conditions.clear();
			contions.put("state", 2);
			List<Map<String, Object>> 
weiboDailyStatisticsDTOs=this.weiboDailyStatisticsService.getUserWeiboData(contions);
			for (Map<String, Object> map : 
weiboDailyStatisticsDTOs) {
				Integer 
userId=BaseUtils.parseToInt(map.get("userId"));
				ClipUsersDTO userDto = 
usersMap.get(userId);
				if (userDto == null) {
					continue;
				}
				
				String 
fid=BaseUtils.parseToString(map.get("fid"));
				String 
mid=BaseUtils.parseToString(map.get("mid"));
				String 
openId=BaseUtils.parseToString(map.get("openId"));
				String 
token=BaseUtils.parseToString(map.get("token"));
				
				String url = 
"https://c.api.weibo.com/2/statuses/statistic/biz.json";
				String param = "access_token=" + token + 
"&id=" + mid+ "&starttime=" + startTime+ "&endtime=" + nextEndTime;
				String result = 
ConnectionUtils.sendGet(url, param);
				WeiboDataDTO weiboData = 
TimedTask.getListWeiboDataDTOByGson(result);
				
				String url1 = 
"https://c.api.weibo.com/2/videos/play_counts/biz.json";
				String param1 = "access_token=" + token 
+ "&oid=" + fid+ "&starttime=" + startTime+ "&endtime=" + nextEndTime;
				String result1 = 
ConnectionUtils.sendGet(url1, param1);
				WeiboPlayDataDTO weiboPlayData = 
TimedTask.getListWeiboPlayDataDTOByGson(result1);

				WeiboDailyStatisticsDTO wDto=new 
WeiboDailyStatisticsDTO();
				
wDto.setOpenId(BaseUtils.parseToLong(openId));
				wDto.setMid(BaseUtils.parseToLong(mid));
			    
wDto.setRepostsCount(BaseUtils.parseToInt(weiboData.getResult().get("receive_repost_count")));
				
wDto.setCommentsCount(BaseUtils.parseToInt(weiboData.getResult().get("receive_comment_count")));
				
wDto.setAttitudesCount(BaseUtils.parseToInt(weiboData.getResult().get("receive_like_count")));
				
wDto.setLinkClickCount(BaseUtils.parseToInt(weiboData.getResult().get("link_click_count")));
				
wDto.setPictureClickCount(BaseUtils.parseToInt(weiboData.getResult().get("picture_click_count")));
				
wDto.setVideoClickCount(BaseUtils.parseToInt(weiboData.getResult().get("video_click_count")));
				
wDto.setReadsCount(BaseUtils.parseToInt(weiboData.getResult().get("receive_read_count")));
				
wDto.setReachCount(BaseUtils.parseToInt(weiboData.getResult().get("receive_reach_count")));
				Date createTime=(Date) 
weiboData.getResult().get("day");
				Date updateTime=(Date) 
weiboData.getResult().get("day");
				
wDto.setCreatedTimestamp(BaseUtils.parseToInt((createTime.getTime())));
				
wDto.setUpdatedTimestamp(BaseUtils.parseToInt((updateTime.getTime())));
				
wDto.setVv(BaseUtils.parseToInt(weiboPlayData.getResult().get("play")));
				
wDto.setPlayCnt(BaseUtils.parseToInt(weiboPlayData.getResult().get("play_cnt")));
				Date day=new Date();     
				
wDto.setStatsTimestamp(BaseUtils.parseToInt(day.getTime()));
				weiboDailyStatisticsDtoList.add(wDto);
			}
			//insert
			
weiboDailyStatisticsService.addWeiboMessage(weiboDailyStatisticsDtoList);
		}catch(Exception e) {
			e.printStackTrace();
			logger.error("TimedTask error:" + 
e.getMessage());
		} finally {
			isNext = true;
			
logger.info("------------TimedTask-------end-------------");
		}
	}
	public static  WeiboDataDTO getListWeiboDataDTOByGson(String 
jsonString) {  
        WeiboDataDTO list = new WeiboDataDTO();  
        Gson gson = new Gson();  
        list = gson.fromJson(jsonString, new TypeToken<WeiboDataDTO>() {  
        }.getType());  
        return list;  
    } 
	public static  WeiboPlayDataDTO 
getListWeiboPlayDataDTOByGson(String jsonString) {  
        WeiboPlayDataDTO list = new WeiboPlayDataDTO();  
        Gson gson = new Gson();  
        list = gson.fromJson(jsonString, new 
TypeToken<WeiboPlayDataDTO>() {  
        }.getType());  
        return list;  
    } 
}
