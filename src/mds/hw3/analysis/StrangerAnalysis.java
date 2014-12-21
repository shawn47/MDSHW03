package mds.hw3.analysis;
import java.util.ArrayList;

import com.renren.api.RennException;

import mds.hw3.common.UserInfo;
import mds.hw3.db.*;
import mds.hw3.renren.*;
public class StrangerAnalysis {
	private static long suid, tuid;
	private static int count = 0;

	public static DBProcesser dbprocesser = new DBProcesser();
	public static RenrenSniper rrsniper = new RenrenSniper();

	public static void main(String[] args) throws RennException, InterruptedException {

		rrsniper.authentication();
		suid = 313620754;
		UserInfo uInfo = new UserInfo();
		uInfo = rrsniper.getUserInfo(suid);
		ArrayList<UserInfo> usersInfo = new ArrayList<>();
		//usersInfo = rrsniper.getFriendList(suid);
		tuid = 220929689;
		UserInfo tInfo = new UserInfo();
		tInfo = rrsniper.getUserInfo(tuid);
		ArrayList<UserInfo> tusersInfo = new ArrayList<>();
		tusersInfo = rrsniper.getFriendList(tuid);
		
		dbprocesser.startDb();
//		dbprocesser.getPath(uInfo, tInfo);
		ArrayList<UserInfo> suggestedFriends = dbprocesser.friendsSuggest(uInfo);
		System.out.println("the following are the friends we've suggested for you:");
		for (int i = 0; i < suggestedFriends.size(); i++) {
			System.out.println("user name: " + suggestedFriends.get(i).getUsername() + "\t" + "userid: " + String.valueOf(suggestedFriends.get(i).getUserid()) + ".");
			
		}
		if (dbprocesser.hasRels(uInfo, tInfo)) {
			System.out.println("user " + String.valueOf(uInfo.getUserid()) + " is a friend of user " + String.valueOf(tInfo.getUserid()) + ".");
		}
		else {
			dbprocesser.createDb(tInfo, tusersInfo);
			dbprocesser.shortestPath(uInfo.getUserid(), tInfo.getUserid());
		}
		
		//dbprocesser.cypherQuery();
		
//		graphDBcreate(uInfo, usersInfo, 0);
		dbprocesser.deleteTargetUser(tInfo, tusersInfo);
		dbprocesser.shutdownDb();
	}
	
	public static void graphDBcreate(UserInfo uInfo, ArrayList<UserInfo> usersInfo, int degree) throws RennException, InterruptedException {
		if (degree >= 2) {
			return;
		}
		else {
			dbprocesser.createDb(uInfo, usersInfo);
			degree++;
			for (int i = 0; i < usersInfo.size(); i++) {
				graphDBcreate(usersInfo.get(i), rrsniper.getFriendList(usersInfo.get(i).getUserid()), degree);
				count++;
				System.out.println("Count:"+count);
			}
		}
	}
}
