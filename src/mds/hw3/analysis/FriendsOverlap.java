package mds.hw3.analysis;

import java.util.ArrayList;

import com.renren.api.AuthorizationException;
import com.renren.api.RennException;

import mds.hw3.common.UserInfo;
import mds.hw3.db.DBProcesser;
import mds.hw3.renren.RenrenSniper;

public class FriendsOverlap {
	private static long myuid;
	private static long targetuid;
	private static int degreesOfSeperations = 0;

	public static DBProcesser dbprocesser = new DBProcesser();
	public static RenrenSniper rrsniper = new RenrenSniper();
	public static void main(String[] args) throws RennException {
		// TODO Auto-generated method stub
		rrsniper.authentication();
		myuid = 313620754;
		targetuid = 220929689;
		UserInfo myInfo = new UserInfo();
		myInfo = rrsniper.getUserInfo(myuid);
		ArrayList<UserInfo> myFriendsUsersInfo = new ArrayList<>();
		myFriendsUsersInfo = rrsniper.getFriendList(myuid);
		
		UserInfo targetInfo = new UserInfo();
		targetInfo = rrsniper.getUserInfo(targetuid);
		ArrayList<UserInfo> targetFriendsUsersInfo = new ArrayList<>();
		targetFriendsUsersInfo = rrsniper.getFriendList(targetuid);
		
		dbprocesser.startDb();
		dbprocesser.createDb(myInfo, myFriendsUsersInfo);
		
		if (dbprocesser.hasRels(myInfo, targetInfo)) {
			System.out.println("220929689 is your friend!");
		}
		else {
			float overlap = dbprocesser.friendsOverlapCalculator(myFriendsUsersInfo.size(), targetFriendsUsersInfo);
			System.out.println("the overlap between you and 220929689 is "+ overlap +" !");
			
		}
		
		dbprocesser.shutdownDb();
	}

}
