package com.company;

import java.io.IOException;

public class Encyclopedia {

    private Dictionary dictionary;

    /**
     * This constructor to create new dictionary (also new database file)
     *
     */
    public Encyclopedia(String dictionaryName) throws IOException, DictionaryException {
        dictionary = new Dictionary(dictionaryName, 1024);
    }

    /**
     * This constructor used to open existing dictionary by given name
     *
     */
    public Encyclopedia(String dictionaryName, String accessFlags) throws IOException, DictionaryException {
        dictionary = new Dictionary(dictionaryName, accessFlags);
    }

    public static void main(String[] args) throws IOException, DictionaryException, ClassNotFoundException {

        /*
        * The following part is to create new dictionary named "dictionary1" and insert new phrase and explanation for it
        *
        * NOTE: the second run will throw exception Database already exists. So to test other functions: search, update, delete
        * you should comment out below code which creating new database with name dictionary1, OR just rename dictionary1 with other name.
        * If you rename database when create, you have to make sure open correct database when search, update, delete
        *
        * */
        Encyclopedia encyclopedia = new Encyclopedia("dictionary1");

        Post post = new Post();
        post.setPhrase("Random File Access");
        post.setExplanation("The terms random access and sequential access are often used to describe data files. " +
                "A random-access data file enables you to read or writeinformation anywhere in the file. " +
                "In a sequential-access file, you can only read and write information sequentially, " +
                "starting from the beginning of the file.");

        encyclopedia.insertNewPost(post);


        /*
        * The following part is to open dictionary named "dictionary1" and
        * after that search a phrase "Random File Access" and print the explanation to console
        * */
        Encyclopedia encyclopedia2 = new Encyclopedia("dictionary1", "rw");
        Post searchResult = encyclopedia2.searchPhrase("Random File Access");
        if(searchResult != null){
            System.out.println(searchResult.getExplanation());
        }


    }



    /**
     * Create new phrase and it's explanation
     *
     */
    public void insertNewPost(Post post) throws IOException, DictionaryException {
        PostWriter postWriter = new PostWriter(post.getPhrase());
        postWriter.writeObject(post.getExplanation());

        dictionary.insertRecord(postWriter);

    }

    /**
     * Update explanation for existing post
     *
     */
    public void updatePost(Post post) throws IOException, DictionaryException {
        PostWriter postWriter = new PostWriter(post.getPhrase());
        postWriter.writeObject(post.getExplanation());

        dictionary.updateRecord(postWriter);

    }

    /**
     * Search explanation for given phrase
     *
     */
    public Post searchPhrase(String phraseToSearch) throws IOException, DictionaryException, ClassNotFoundException {
        PostReader postReader = dictionary.readRecord(phraseToSearch);
        String explanation = (String) postReader.readObject();

        Post result = new Post();
        result.setPhrase(phraseToSearch);
        result.setExplanation(explanation);

        return result;
    }

    /**
     * Delete a post by given phrase
     *
     */
    public void deletePost(String phrase) throws IOException, DictionaryException {
        dictionary.deleteRecord(phrase);

    }
}
