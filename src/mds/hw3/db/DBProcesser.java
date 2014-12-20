package mds.hw3.db;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

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
        //String storeDir = TargetDirectory.forTest( PathFindingDocTest.class ).makeGraphDbDir().getAbsolutePath();
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
    	//Node n1 = nodeIndex.get(PRIMARY_KEY, String.valueOf(u1.getUserid())).getSingle();
    	//Node n2 = nodeIndex.get(PRIMARY_KEY, String.valueOf(u2.getUserid())).getSingle();
    	if (hasRels(u1, u2)){
    		System.out.println("u1 u2 has rels");
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
    		//return (int) rootNode.getProperty("level");
    	}
    }
    
    private void createRelationshipsBetween( final Node... nodes )
    {
        for ( int i = 0; i < nodes.length - 1; i++ )
        {
            nodes[i].createRelationshipTo( nodes[i+1], RelTypes.KNOWS );
        }
    }
    
 // find using index
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
    
    public void shortestPath(long uid1, long uid2) {
    	try ( Transaction tx = graphDb.beginTx() )
        {
    		Node node1 = getNodeByIndex(String.valueOf(uid1));
    		Node node2 = getNodeByIndex(String.valueOf(uid2));
	        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
	            PathExpanders.forTypeAndDirection(RelTypes.KNOWS, Direction.OUTGOING ), 15 );
	        Iterable<Path> paths = finder.findAllPaths( node1, node2 );
	        // END SNIPPET: shortestPathUsage
	        Path path = paths.iterator().next();
	        Iterator<Node> iterator = path.nodes().iterator();
	        iterator.next();
	        
	        tx.success();
        }
    }
    
    private Node getNodeByIndex(String uid) {
    	Node n = nodeIndex.get(PRIMARY_KEY, uid).getSingle();
        return n;
    }
    
    public void cypherQuery() {
    	ExecutionEngine engine = new ExecutionEngine(graphDb);

    	ExecutionResult result;
    	try (Transaction ignored = graphDb.beginTx()) {
    		result = engine.execute( "match (n) return n" );
    		System.out.println(result.iterator().next());
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
