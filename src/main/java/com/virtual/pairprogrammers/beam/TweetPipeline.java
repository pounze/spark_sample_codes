package com.virtual.pairprogrammers.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TweetPipeline
{
    public static void main(String ...args)
    {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        // read tweets fromn json file

        PCollection<String> lines = pipeline.apply(TextIO.read().from("google_tweets_small.json"));

        // parse json

        PCollection<Tweet> tweets = lines.apply(ParseJsons.of(Tweet.class));

        // extract usernames

        //tweets.apply(MapElements.into(TypeDescriptors.strings()).via(t -> t.User));

        // discard usernames having tweet count less than 6

        // write the usernames to a file

        pipeline.run().waitUntilFinish();
    }
}
