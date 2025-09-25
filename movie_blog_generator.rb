#!/usr/bin/env ruby
require 'logger'
require 'active_support'
require 'mongoid'
require 'httparty'
require 'nokogiri'
require 'json'
require 'logger'
require 'aws-sdk-bedrockruntime'

# Configure Mongoid
Mongoid.load!('/home/ubuntu/blog/mongoid.yml', :development)

# Movie model - read-only (only title)
class MovieTheme
    include Mongoid::Document
    store_in client: "catalog"
    field :title, type: String
end

# Blog model for storing generated content
class Blog
    include Mongoid::Document
    field :title, type: String
    field :language, type: String
    field :duration, type: String
    field :rating, type: String
    field :director, type: String
    field :reviews, type: Array
    field :synopsis, type: String
    field :why_watch, type: String
    field :where_watch, type: String
    field :cast, type: Array
    field :hash_tag, type: Array
end


class MovieBlogGenerator
  BEDROCK_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
  SERPER_API_KEY = "9f0b257743ae72345ad180af47b97fd8e1e06796"
  
  def initialize
    @logger = Logger.new(STDOUT)
    @bedrock = Aws::BedrockRuntime::Client.new(region: 'us-east-1')
  end

  def search_movie_details(title)
    query = "#{title} movie plot cast director year genre"
    
    begin
      response = HTTParty.post('https://google.serper.dev/search',
        headers: {
          'X-API-KEY' => SERPER_API_KEY,
          'Content-Type' => 'application/json'
        },
        body: { q: query }.to_json,
        timeout: 10
      )
      
      return default_movie_data(title) unless response.success?
      
      results = JSON.parse(response.body)
      extract_movie_info_from_results(title, results)
    rescue => e
      @logger.error "API error for #{title}: #{e.message}"
      return default_movie_data(title)
    end
  end

  def default_movie_data(title)
    {
      year: '2020',
      genre: 'Drama',
      director: 'Unknown Director',
      cast: ['Actor 1', 'Actor 2', 'Actor 3'],
      plot: "#{title} is an engaging movie with compelling storyline and great performances.",
      duration: '120 min',
      language: 'Hindi',
      content_rating: 'U/A',
      poster_url: nil,
      watch_url: "https://shemaroome.com/movies/#{title.downcase.gsub(' ', '-')}"
    }
  end

  def extract_movie_info_from_results(title, results)
    # Extract from search results
    text = results.dig('organic')&.first(3)&.map { |r| r['snippet'] }&.join(' ') || ''
    
    {
      year: extract_year(text),
      genre: extract_genre(text),
      director: extract_director(text, title),
      cast: extract_cast(text, title),
      plot: extract_plot(text, title),
      duration: extract_duration(text),
      language: 'English',
      content_rating: 'U/A',
      poster_url: nil,
      watch_url: "https://shemaroome.com/movies/#{title.downcase.gsub(' ', '-')}"
    }
  end

  def extract_year(text)
    match = text.match(/(19|20)\d{2}/)
    match ? match[0] : '2020'
  end

  def extract_genre(text)
    genres = ['Action', 'Drama', 'Comedy', 'Thriller', 'Romance', 'Horror', 'Adventure']
    found = genres.find { |g| text.downcase.include?(g.downcase) }
    found || 'Drama'
  end

  def extract_director(text, title)
    # Look for "directed by" patterns
    match = text.match(/directed by ([A-Z][a-z]+ [A-Z][a-z]+)/i)
    match ? match[1] : 'Unknown Director'
  end

  def extract_cast(text, title)
    # Look for actor names (capitalized words)
    actors = text.scan(/\b[A-Z][a-z]+ [A-Z][a-z]+\b/).uniq.first(5)
    actors.any? ? actors : ['Actor 1', 'Actor 2', 'Actor 3']
  end

  def extract_plot(text, title)
    # Clean and return relevant text as plot
    plot = text.gsub(/\s+/, ' ').strip
    plot.length > 50 ? plot[0..500] : "#{title} is an engaging movie with compelling storyline and great performances."
  end

  def extract_duration(text)
    match = text.match(/(\d+)\s*(?:min|minutes|hrs?|hours?)/i)
    match ? "#{match[1]} min" : '120 min'
  end

  def generate_reviews(movie_data)
    public_reviews = [
      {
        author: 'FilmCritic42',
        rating_range: (3.5..5.0),
        content: 'A masterpiece of %{genre} cinema. The direction is impeccable, and the performances, especially by %{actor}, are outstanding. The story flows naturally and keeps you engaged throughout its runtime.',
        sentiment: 'positive'
      },
      {
        author: 'MovieBuff99',
        rating_range: (4.0..5.0),
        content: 'One of the best %{genre} films I\'ve seen in years. %{director}\'s vision shines through in every scene. The cinematography is breathtaking, and the score perfectly complements the narrative.',
        sentiment: 'positive'
      },
      {
        author: 'CinemaEnthusiast',
        rating_range: (3.0..4.5),
        content: 'A solid %{genre} film that delivers what it promises. %{actor}\'s performance is the highlight, bringing depth to an otherwise standard character. The pacing is good, though some scenes could have been tightened.',
        sentiment: 'positive'
      },
      {
        author: 'ScreenTime',
        rating_range: (2.0..3.5),
        content: 'An average %{genre} movie with some memorable moments. The plot is somewhat predictable, but %{actor} manages to elevate the material. The direction by %{director} is competent if not particularly innovative.',
        sentiment: 'neutral'
      },
      {
        author: 'ReelReviewer',
        rating_range: (1.5..3.0),
        content: 'A disappointing entry in the %{genre} category. Despite %{actor}\'s best efforts, the script lacks coherence and the direction feels uninspired. Some good ideas get lost in the execution.',
        sentiment: 'negative'
      }
    ]
    
    reviews = []
    review_count = [public_reviews.length, 10].min
    review_count = rand([3, review_count - 2].max..review_count)
    
    selected_reviews = public_reviews.sample(review_count)
    
    selected_reviews.each do |review_template|
      actor = movie_data[:cast]&.sample || 'the lead actor'
      rating = rand(review_template[:rating_range]).round(1)
      
      content = review_template[:content] % {
        genre: movie_data[:genre]&.downcase || 'film',
        actor: actor,
        director: movie_data[:director] || 'the director'
      }
      
      if rand > 0.7
        suffix = review_template[:sentiment] == 'positive' ? 'one to miss' : 'not one to miss'
        content += " '#{movie_data[:title]}' is #{suffix}."
      end
      
      review_date = rand(1..365).days.ago.strftime('%Y-%m-%d')
      
      reviews << {
        author: review_template[:author],
        rating: rating,
        content: content,
        date: review_date,
        source: 'Public Review Database'
      }
    end
    
    reviews
  end

  def enhance_with_ai(movie_data)
    {
      why_watch: generate_why_watch(movie_data),
      seo_hashtags: generate_hashtags(movie_data),
      seo_synopsis: rewrite_synopsis(movie_data),
      where_to_watch: generate_where_to_watch(movie_data)
    }
  end

  def generate_blogs
    @logger.info "Generating blog data..."
    
MovieTheme.where(:status => "published",:business_group_id => "548343938", :app_ids => "350502978", :episode_type => "movie" ).to_a..limit(1).each do |movie|

      @logger.info "Processing: #{movie.title}"
      
      movie_data = search_movie_details(movie.title)
      movie_data[:title] = movie.title
      
      reviews = generate_reviews(movie_data)
      ai_content = enhance_with_ai(movie_data)
      
      Blog.create!(
        title: movie_data[:title],
        language: movie_data[:language],
        duration: movie_data[:duration],
        rating: movie_data[:content_rating],
        director: movie_data[:director],
        reviews: reviews,
        synopsis: ai_content[:seo_synopsis] || movie_data[:plot],
        why_watch: ai_content[:why_watch],
        where_watch: ai_content[:where_to_watch],
        cast: movie_data[:cast],
        hash_tag: ai_content[:seo_hashtags] || []
      )
      
      @logger.info "Saved blog data for: #{movie.title}"
      sleep(3)
    end
    
    @logger.info "All blog data saved to database"
  end

  def run_pipeline
    @logger.info "Starting pipeline with existing MongoDB data..."
    generate_blogs
    @logger.info "Pipeline completed!"
  end

  private

  def generate_why_watch(movie_data)
    prompt = "Write a compelling 'Why You Should Watch' section for #{movie_data[:title]} (#{movie_data[:year]}). Genre: #{movie_data[:genre]}. Plot: #{movie_data[:plot]}. Keep it 150-200 words, engaging and professional."
    
    invoke_bedrock(prompt)
  end

  def generate_hashtags(movie_data)
    prompt = "Generate 15-20 SEO hashtags for #{movie_data[:title]} (#{movie_data[:year]}) #{movie_data[:genre]} movie for ShemarooMe platform. Return only hashtags separated by spaces."
    
    response = invoke_bedrock(prompt)
    response&.split&.select { |tag| tag.start_with?('#') }&.first(20)
  end

  def rewrite_synopsis(movie_data)
    prompt = "Rewrite this movie synopsis to be SEO-friendly and engaging: #{movie_data[:plot]}. Movie: #{movie_data[:title]} (#{movie_data[:year]}). Keep it 50-100 words."
    
    invoke_bedrock(prompt)
  end

  def generate_where_to_watch(movie_data)
    prompt = "Write a compelling 'Where to Watch' description for #{movie_data[:title]} on ShemarooMe streaming platform. Include platform benefits like 4K quality, unlimited access, multiple devices, offline viewing, and exclusive content. Keep it 100-150 words, promotional and engaging."
    
    invoke_bedrock(prompt)
  end

  def invoke_bedrock(prompt)
    request = {
      anthropic_version: 'bedrock-2023-05-31',
      max_tokens: 500,
      temperature: 0.7,
      messages: [{ role: 'user', content: prompt }]
    }
    
    response = @bedrock.invoke_model(
      model_id: BEDROCK_MODEL_ID,
      body: request.to_json
    )
    
    JSON.parse(response.body.read).dig('content', 0, 'text')&.strip
  rescue => e
    @logger.error "Bedrock error: #{e.message}"
    nil
  end

end

# CLI interface
if __FILE__ == $0
  generator = MovieBlogGenerator.new
  generator.run_pipeline
end
