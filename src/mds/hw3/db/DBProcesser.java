package mds.hw3.db;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import mds.hw3.common.UserInfo;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.IteratorUtil;


public class DBProcesser {
	private static final String PATH_DB = "target/friend-db";
	private static GraphDatabaseService graphDb;
    private static final String PRIMARY_KEY = "userid";
    private static Index<Node> nodeIndex;
    
    public static enum RelTypes implements RelationshipType {
        NEO_NODE,
        KNOWS
    }
    
    public void startDb() {
        //deleteFileOrDirectory(new File(PATH_DB));
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(PATH_DB);
        try ( Transaction tx = graphDb.beginTx() ) {
        	nodeIndex = graphDb.index().forNodes("nodes");
        	tx.success();
        }
    }
    
    private Node createNode(UserInfo uInfo, int degree) {
    	// create node
		Node node = graphDb.createNode();
		node.setProperty("username", uInfo.getUsername());
		node.setProperty(PRIMARY_KEY, String.valueOf(uInfo.getUserid()));
		node.setProperty("level", String.valueOf(degree));
		// add to index
        nodeIndex.add(node, PRIMARY_KEY, String.valueOf(uInfo.getUserid()));
    	return node;
    }
    
    public void getPath(UserInfo u1, UserInfo u2) {
    	if (hasRels(u1, u2)){
    		System.out.println("u1 is a friend of u2");
    	}
    	else {
    		shortestPath(u1.getUserid(), u2.getUserid());
    	}
    }
    
    public void createDb(UserInfo rootUserInfo, ArrayList<UserInfo> usersInfo) {
    	try (Transaction tx = graphDb.beginTx()) {
    		Node rootNode;
    		Node tmpNode = nodeIndex.get(PRIMARY_KEY, String.valueOf(rootUserInfo.getUserid())).getSingle();
    		if (tmpNode != null) {
    			rootNode = tmpNode;
    		}
    		else {
    			rootNode = createNode(rootUserInfo, 1);
    		}
    		
    		for (int i = 0; i < usersInfo.size(); i++) {
    			Node tmpfNode = nodeIndex.get(PRIMARY_KEY, String.valueOf(usersInfo.get(i).getUserid())).getSingle();
    			if (tmpfNode != null) {
    				createRelationshipsBetween(rootNode, tmpfNode);
    			}
    			else {
    				Node friendNode = createNode(usersInfo.get(i), Integer.parseInt((String) rootNode.getProperty("level")) + 1);
        			createRelationshipsBetween(rootNode, friendNode);
    			}
    			
    		}
    		tx.success();
    	}
    }
    
    private void createRelationshipsBetween( final Node... nodes )
    {
        for ( int i = 0; i < nodes.length - 1; i++ )
        {
            nodes[i].createRelationshipTo( nodes[i+1], RelTypes.KNOWS );
        }
    }
    
    public ArrayList<UserInfo> friendsSuggest(UserInfo uinfo) {
    	ExecutionEngine engine = new ExecutionEngine(graphDb);
    	ExecutionResult result;
    	String cypherToSELECT;
    	int max = 0;
    	ArrayList<UserInfo> maxDegreeNodes = new ArrayList<>();
    	ArrayList<Node> suggestedFriends = new ArrayList<>();
    	try (Transaction ignored = graphDb.beginTx()) {
    		cypherToSELECT = "match (n) return n";
    		result = engine.execute(cypherToSELECT);
    		
    		Iterator<Node> n_column = result.columnAs("n");
            for ( Node node : IteratorUtil.asIterable( n_column ) )
            {
            	System.out.println("name: " + node.getProperty("username") + "\t degree: " + node.getDegree());
            	UserInfo utmp = new UserInfo();
            	utmp.setUserid(Long.parseLong((String)node.getProperty(PRIMARY_KEY)));
            	utmp.setUsername((String)node.getProperty("username"));
            	
            	if (!(hasRels2(uinfo, utmp)) && (uinfo.getUserid() != utmp.getUserid())) {
            		if (node.getDegree() >= max) {
            			max = node.getDegree();
            			//maxDegreeNodes.add(node);
            		}
            	}
            }
            result = engine.execute(cypherToSELECT);
    		n_column = result.columnAs("n");
            for ( Node node : IteratorUtil.asIterable( n_column ) )
            {
            	if (node.getDegree() == max) {
            		suggestedFriends.add(node);
            		UserInfo u = new UserInfo();
            		u.setUserid(Long.parseLong((String)node.getProperty(PRIMARY_KEY)));
            		u.setUsername((String)node.getProperty("username"));
            		maxDegreeNodes.add(u);
            	}
            }
    		ignored.success();
    		return maxDegreeNodes;
    	}
    }
    
 // find using label
    public Node findNodeUsingIndex(String userid) {
    	Label label = DynamicLabel.label( "USER" );
        int idToFind = Integer.parseInt(userid);
        String uidToFind = userid;
        try ( Transaction tx = graphDb.beginTx() )
        {
            try ( ResourceIterator<Node> users =
                    graphDb.findNodesByLabelAndProperty( label, "uid", uidToFind ).iterator() )
            {
                ArrayList<Node> userNodes = new ArrayList<>();
                while ( users.hasNext() )
                {
                    userNodes.add( users.next() );
                }

                for ( Node node : userNodes )
                {
                    System.out.println( "The username of userid " + idToFind + " is " + node.getProperty( "name" ) );
                    return node;
                }
                userNodes.clear();
            }
        }
        return null;
    }
    
    public boolean hasRels(UserInfo uinfo1, UserInfo uinfo2) {
    	try (Transaction tx = graphDb.beginTx()) {
	    	Node nodequery = nodeIndex.get(PRIMARY_KEY, String.valueOf(uinfo2.getUserid())).getSingle();
	    	if (nodequery == null) {
	    		tx.success();
	    		return false;
	    	}
	    	else {
	    		tx.success();
	    		return true;
	    	}
    	}
    }
    
    public boolean hasRels2(UserInfo uinfo1, UserInfo uinfo2) {
    	int length = getShortestPathLength(uinfo1.getUserid(), uinfo2.getUserid());
    	if (length == 1) {
    		return true;
    	}
    	else {
    		return false;
    	}
    }
    
    public float friendsOverlapCalculator(int myFriendsNum, ArrayList<UserInfo> usersInfo) {
    	try (Transaction tx = graphDb.beginTx()) {
	    	int overLapCount = 0;
	    	ArrayList<Node> overlapFirends = new ArrayList<>();
	    	for (int i = 0;i < usersInfo.size(); i++) {
	    		Node n = getNodeByIndex(String.valueOf(usersInfo.get(i).getUserid()));
	    		if (n != null) {
	    			overlapFirends.add(n);
	    			overLapCount++;
	    		}
	    	}
	    	tx.success();
	    	return ((float)overLapCount/(float)myFriendsNum);
    	}
    }
    
    public int getShortestPathLength(long uid1, long uid2) {
    	try ( Transaction tx = graphDb.beginTx() )
        {
    		Node node1 = getNodeByIndex(String.valueOf(uid1));
    		Node node2 = getNodeByIndex(String.valueOf(uid2));
	        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
	            PathExpanders.forTypeAndDirection(RelTypes.KNOWS, Direction.BOTH ), 15);
	        Iterable<Path> paths = finder.findAllPaths( node1, node2 );
	        int minLength = 100;
	        if (paths == null) {
	        	String message = "no path from "+ node1.getProperty("username")+" to "+node2.getProperty("username") + "!";
	        	System.out.println(message);
	        }
	        else {
	        	Path path;
		        Iterator<Path> t = paths.iterator();
		        Iterator<Node> nodeIterator;
		        Node node;
		        while (t.hasNext()) {
		        	path = t.next();
		        	if (path.length() < minLength)
		        		minLength = path.length();
		        }
	        }
	        
	        tx.success();
	        return minLength;
        }
    }
    
    public void shortestPath(long uid1, long uid2) {
    	ArrayList<Path> friendshipBuildingPath = new ArrayList<>();
    	try ( Transaction tx = graphDb.beginTx() )
        {
    		Node node1 = getNodeByIndex(String.valueOf(uid1));
    		Node node2 = getNodeByIndex(String.valueOf(uid2));
	        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
	            PathExpanders.forTypeAndDirection(RelTypes.KNOWS, Direction.BOTH ), 15);
	        Iterable<Path> paths = finder.findAllPaths( node1, node2 );
	        if (paths == null) {
	        	String message = "no path from "+ node1.getProperty("username")+" to "+node2.getProperty("username") + "!";
	        	System.out.println(message);
	        }
	        else {
	        	Path path;
		        Iterator<Path> t = paths.iterator();
		        Iterator<Node> nodeIterator;
		        Node node;
		        while (t.hasNext()) {
		        	
		        	path = t.next();
		        	friendshipBuildingPath.add(path);
		        	System.out.println("A path is: " + String.valueOf(path.length()));
		        	nodeIterator = path.nodes().iterator();
		        	while (nodeIterator.hasNext()) {
		        		node = nodeIterator.next();
		        		if (node.getId() != path.endNode().getId()) {
		        			System.out.print(node.getProperty("username") + "=>");
		        		}
		        		else {
		        			System.out.println(node.getProperty("username") + ".");
		        		}
		        	}
		        }
	        }
	        
	        tx.success();
        }
    }
    
    private Node getNodeByIndex(String uid) {
    	Node n = nodeIndex.get(PRIMARY_KEY, uid).getSingle();
        return n;
    }
    
    public void deleteTargetUser(UserInfo uinfo, ArrayList<UserInfo> usinfo) {
    	ExecutionEngine engine = new ExecutionEngine(graphDb);
    	ExecutionResult result;
    	String cypherToDelete;
    	
    	try (Transaction ignored = graphDb.beginTx()) {
    		cypherToDelete = "START n=node(*) MATCH n-[rel:" + RelTypes.KNOWS + "]->r WHERE n.userid='" + String.valueOf(uinfo.getUserid()) + "' DELETE rel";
    		result = engine.execute(cypherToDelete);
    		long edge = 86;
    		Node node1 = getNodeByIndex(String.valueOf(uinfo.getUserid()));
    		for (int i = 0; i < usinfo.size(); i++) {
    			Node node2 = getNodeByIndex(String.valueOf(usinfo.get(i).getUserid()));
    			if (node2 != null) {
    				//if (uinfo.getUserid() < usinfo.get(i).getUserid()) {
        			//System.out.println(node1.getId() + "\t" + node2.getId());
        			if (edge < node2.getId()) {
        				cypherToDelete = "START n=node(*) MATCH n WHERE n.userid='" + String.valueOf(usinfo.get(i).getUserid()) + "' DELETE n";
        				result = engine.execute(cypherToDelete);
        			}
    			}
    		}
    		cypherToDelete = "START n=node(*) MATCH n WHERE n.userid='" + String.valueOf(uinfo.getUserid()) + "' DELETE n";
    		result = engine.execute(cypherToDelete);
    		/**
    		cypherToDelete = "START n=node(*) MATCH n WHERE n.userid='3146' DELETE n";
			result = engine.execute(cypherToDelete);
			cypherToDelete = "START n=node(*) MATCH n WHERE n.userid='3854' DELETE n";
			result = engine.execute(cypherToDelete);
			cypherToDelete = "START n=node(*) MATCH n WHERE n.userid='4466' DELETE n";
			result = engine.execute(cypherToDelete);
    		*/
    		ignored.success();
    	}
    }
    
    public void cypherQuery() {
    	ExecutionEngine engine = new ExecutionEngine(graphDb);

    	ExecutionResult result;
    	int cnt = 0;
    	try (Transaction ignored = graphDb.beginTx()) {
    		result = engine.execute( "match (n) return n" );
    		Node n;
    		while (result.iterator().next() != null) {
    			cnt++;
    			if (cnt == 23) {
    				
    			}
    		}
    		System.out.println(cnt);
    		ignored.success();
    	}
    }
    
    public void shutdownDb() {
        try {
            if (graphDb != null) graphDb.shutdown();
        }
        finally
        {
            graphDb = null;
        }
    }
    
    public static void deleteFileOrDirectory(File file)
    {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            for (File child: file.listFiles()) {
                deleteFileOrDirectory(child);
            }
        }
        else {
            file.delete();
        }
    }
}
