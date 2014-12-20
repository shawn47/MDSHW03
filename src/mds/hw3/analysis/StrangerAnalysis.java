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
//		usersInfo = rrsniper.getFriendList(suid);
		tuid = 4136;
		UserInfo tInfo = new UserInfo();
		tInfo = rrsniper.getUserInfo(tuid);
		
		dbprocesser.startDb();
//		dbprocesser.getPath(uInfo, tInfo);
//		dbprocesser.cypherQuery();
		
		graphDBcreate(uInfo, usersInfo, 0);
//		
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
