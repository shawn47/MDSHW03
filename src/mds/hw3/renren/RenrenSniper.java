package mds.hw3.renren;

import java.util.ArrayList;

import mds.hw3.common.UserInfo;

import com.renren.api.AuthorizationException;
import com.renren.api.RennClient;
import com.renren.api.RennException;
import com.renren.api.service.User;


public class RenrenSniper {
	@SuppressWarnings("unused")
	//private static String API_ID = "473940";
	//private static String API_KEY = "c8365eab24da467d9350b9bad06c539d";
	//private static String SECRET_KEY = "2260d56f31864bb990374763518eca84";
	
	//private static String API_ID = "473945";
	//private static String API_KEY = "b6a36844842c4574aa53013d0aae4be8";
	//private static String SECRET_KEY = "485c0cb3f1e44060b9ec40baa7585088";
	
	//private static String API_ID = "473945";
	//private static String API_KEY = "c0235196345a470bbcb9294a2f5bcbcd";
	//private static String SECRET_KEY = "dc3acbee826943889683bf2273c5ee15";
	
	private static String API_ID = "473945";
	private static String API_KEY = "c0235196345a470bbcb9294a2f5bcbcd";
	private static String SECRET_KEY = "dc3acbee826943889683bf2273c5ee15";
	private long userid = 313620754;
	private RennClient client;
	
	public void authentication() throws AuthorizationException {
		//User [] user2 = client.getUserService().listUserFriend (Long  userId, Integer  pageSize, Integer  pageNumber)
		client = new RennClient(API_KEY, SECRET_KEY);
		client.authorizeWithClientCredentials();
	}
	
	public UserInfo getUserInfo(long uid) throws RennException {
		User user = client.getUserService().getUser (uid);
		UserInfo uInfo = new UserInfo();
		uInfo.setUserid(uid);
		uInfo.setUsername(user.getName());
		return uInfo;
	}
	
	public ArrayList<UserInfo> getFriendList(long uid) throws InterruptedException {
		//User user = client.getUserService().getUser (uid);
		//if (user == null) {
		//	return null;
		//}
		User [] user2;
		ArrayList<UserInfo> usersInfo = new ArrayList<>();
		//UserInfo uInfo = new UserInfo();
		for (int i = 1; i <= 1 ;i++) {
			try {
				user2 = client.getUserService().listUserFriend (uid, 10, i);
				if (user2.length == 0) {
					break;
				}
				else {
					for (int j = 0; j < user2.length; j++) {
						UserInfo uInfo = new UserInfo();
						uInfo.username = user2[j].getName();
						uInfo.userid = user2[j].getId();
						usersInfo.add(uInfo);
					}
				}
				
				Thread.sleep(2*1000);
			} catch (RennException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
		}
		return usersInfo;
	}
	public void getSomeDegreeOfSeperationFriendList(int degreeOfSeperation, long startUserId) {
		
	}
}
