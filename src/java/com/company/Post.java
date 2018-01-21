package com.company;

import java.io.Serializable;

public class Post implements Serializable {

    String phrase;
    String explanation;

    public Post() {
    }

    public String getPhrase() {
        return phrase;
    }

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public String getExplanation() {
        return explanation;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }
}
