package girafon.ScalableApriori;


class TrieNode {
    TrieNode[] arr;
    
    boolean isEnd;
    // Initialize your data structure here.
    
    public TrieNode(int size) {
        this.arr = new TrieNode[size];
    }
 
}
