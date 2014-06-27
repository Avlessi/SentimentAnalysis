package emoticons;


import java.util.HashSet;
import java.util.Set;

public class SeedWordsHelper {
	
	private static Set<String> posSet = new HashSet<String>();
	private static Set<String> negSet = new HashSet<String>();
	private static Set<String> seedSet = new HashSet<String>();
	
	static	{
		initSeedSet();
	}
	
	private static void initSeedSet(){
		String posEmoticons = ")) :) :)) :-) :-)) ;) =) ;-) ^_^ :] :3 =] :} :^) :っ) :-D :-d :D :d 8-D 8-d 8D 8d x-D x-d xD xd X-D X-d XD =-D =-d =D =d =3 :'-) :')";		
		String negEmoticons = "(( :( :(( >:[ :-( :-(( :-[ :[ :{ -( :'( D:< D: D8 d8 D; D= DX dx v.v D-':";
		
		String [] posList = posEmoticons.split("[ ]+");
		for(int i = 0; i < posList.length; ++i)
			posSet.add(posList[i]);
			
		String [] negList = negEmoticons.split("[ ]+");
		for(int i = 0; i < negList.length; ++i)
			negSet.add(negList[i]);
		
		seedSet.addAll(posSet);
		seedSet.addAll(negSet);
	}
	
	public static boolean isPositive(String word) {
		return posSet.contains(word);
	}
	
	public static boolean isNegative(String word) {
		return negSet.contains(word);
	}
	
	public static boolean isSeedWord(String word) {
		return seedSet.contains(word);
	}
	
	public static Set<String> getSeedSet() {				 
		return seedSet;
	}
	
	public static Set<String> getPosSet(){
		return posSet;
	}
	
	public static Set<String> getNegSet() {
		return negSet;
	}
}