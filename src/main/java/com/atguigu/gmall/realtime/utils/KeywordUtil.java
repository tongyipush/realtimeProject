package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * IK分词类工具
 *
 */
public class KeywordUtil {

    public static List<String> analyze(String text){
        StringReader stringReader = new StringReader(text);

        //todo 使用分词器方法分词，true标识智能分词
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);

        //todo 定义集合放分词结果
        List<String> keywordsList = new ArrayList<>();

        try {
            //todo Lexeme表示分词后封装的对象，里边包括词性：形容词/名词，词的长度
            Lexeme key = null;
            while ( (key=ikSegmenter.next()) != null){
                //拿到当前的单词文本，还可以拿到单词类型，长度
                keywordsList.add(key.getLexemeText());
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("分词失败!");
        }

        return keywordsList;
    }

    //todo 测试
    public static void main(String[] args) {
        List<String> stringList = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
        System.out.println(stringList);
    }
}
