import java.io.Serializable;

/**
 * todo 该对象存入hive中的时候时间格式是随机的,12小时制或24小时制,需要统一
 * 用户行为类，由客户上传或者日志解析。
 * 作为整个算法流程的输入，无法导入在线存储
 */
public class User_Behavior implements Serializable {


	private String userId;
	private String itemId;
	private String bhvType;
	/**
	 * 	行为分数,比如评分,可为空,上传时候以"null"代替
	 */
	private String bhvAmt;
	/**
	 * 	行为次数，默认为1，消费可以是购买件数,可为空,,上传时候以"null"代替
	 */
	private String bhvCnt;
	/**
	 * 	用户对物品的评价文本,可为空,上传时候以"null"代替
	 */
	private String content;

	private String bhvDatetime;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getBhvType() {
		return bhvType;
	}

	public void setBhvType(String bhvType) {
		this.bhvType = bhvType;
	}

	public String getBhvAmt() {
		return bhvAmt;
	}

	public void setBhvAmt(String bhvAmt) {
		this.bhvAmt = bhvAmt;
	}

	public String getBhvCnt() {
		return bhvCnt;
	}

	public void setBhvCnt(String bhvCnt) {
		this.bhvCnt = bhvCnt;
	}

	public String getBhvDatetime() {
		return bhvDatetime;
	}

	public void setBhvDatetime(String bhvDatetime) {
		this.bhvDatetime = bhvDatetime;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public User_Behavior(String userName, String itemId, String bhvType, String bhvAmt, String bhvCnt, String content, String bhvDatetime) {
		this.userId = userName;
		this.itemId = itemId;
		this.bhvType = bhvType;
		this.bhvAmt = bhvAmt;
		this.bhvCnt = bhvCnt;
		this.content = content;
		this.bhvDatetime = bhvDatetime;
	}

	public User_Behavior() {
	}

	public User_Behavior(String userId, String itemId, String bhvAmt) {
		this.userId = userId;
		this.itemId = itemId;
		this.bhvAmt = bhvAmt;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
            return true;
        }
		if (o == null || getClass() != o.getClass()) {
            return false;
        }

		User_Behavior that = (User_Behavior) o;

		if (userId != null ? !userId.equals(that.userId) : that.userId != null) {
            return false;
        }
		if (itemId != null ? !itemId.equals(that.itemId) : that.itemId != null) {
            return false;
        }
		if (bhvType != null ? !bhvType.equals(that.bhvType) : that.bhvType != null) {
            return false;
        }
		if (bhvAmt != null ? !bhvAmt.equals(that.bhvAmt) : that.bhvAmt != null) {
            return false;
        }
		if (bhvCnt != null ? !bhvCnt.equals(that.bhvCnt) : that.bhvCnt != null) {
            return false;
        }
		if (content != null ? !content.equals(that.content) : that.content != null) {
            return false;
        }
		return bhvDatetime != null ? bhvDatetime.equals(that.bhvDatetime) : that.bhvDatetime == null;
	}

	@Override
	public int hashCode() {
		int result = userId != null ? userId.hashCode() : 0;
		result = 31 * result + (itemId != null ? itemId.hashCode() : 0);
		result = 31 * result + (bhvType != null ? bhvType.hashCode() : 0);
		result = 31 * result + (bhvAmt != null ? bhvAmt.hashCode() : 0);
		result = 31 * result + (bhvCnt != null ? bhvCnt.hashCode() : 0);
		result = 31 * result + (content != null ? content.hashCode() : 0);
		result = 31 * result + (bhvDatetime != null ? bhvDatetime.hashCode() : 0);
		return result;
	}

	public int hashCodeForUserName(String name){
		int result= name != null ? name.hashCode() : 0;
		return result;
	}

	@Override
	public String toString() {
		return "User_Behavior{" + "userId='" + userId + '\'' + ", itemId='" + itemId + '\'' + ", bhvType='" + bhvType + '\'' + ", bhvAmt='" + bhvAmt + '\'' + ", bhvCnt='" + bhvCnt + '\'' + ", content='" + content + '\'' + ", bhvDatetime='" + bhvDatetime + '\'' + '}';
	}
}
