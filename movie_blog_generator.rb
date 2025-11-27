#!/usr/bin/env ruby
require 'csv'
require 'ostruct'
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
    field :language, type: String
    field :release_date_string, type: String
end

# Blog model for storing generated content

class Blog
  include Mongoid::Document
  include Mongoid::Timestamps
  store_in client: "catalog"

  field :theme, type: String
  field :theme_id, type: String
  field :title, type: String
  field :language, type: String
  field :duration, type: String
  field :rating, type: String
  field :quality, type: String
  field :director, type: String
  field :cast, type: Array, default: []
  field :synopsis, type: String
  field :why_watch, type: String
  field :where_watch, type: String
  field :reviews, type: Array, default: []
  field :hash_tag, type: Array, default: []
  validates :theme_id, presence: true, uniqueness: { message: "Theme ID must be unique" }

end

class MovieBlogGenerator
  BEDROCK_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
  SERPER_API_KEY = "9f0b257743ae72345ad180af47b97fd8e1e06796"
  
  def initialize
    @logger = Logger.new(STDOUT)
    @bedrock = Aws::BedrockRuntime::Client.new(region: 'us-east-1')
  end

  def extract_year_from_movie_theme(movie_theme)
    return nil unless movie_theme&.release_date_string
    
    # Extract year from various date formats
    year_match = movie_theme.release_date_string.match(/(19|20)\d{2}/)
    year_match ? year_match[0] : nil
  end

  def search_movie_with_theme(movie_theme)
    year = extract_year_from_movie_theme(movie_theme)
    language = movie_theme.language || 'English'
    search_movie_details(movie_theme.title, year, language)
  end

  def search_movie_details(title, year = nil, language = "English")
    query = year ? "\"#{title}\" #{year} #{language} movie cast starring actors" : "\"#{title}\" #{language} movie cast starring actors"
    
    begin
      response = HTTParty.post('https://google.serper.dev/search',
        headers: {
          'X-API-KEY' => SERPER_API_KEY,
          'Content-Type' => 'application/json'
        },
        body: { q: query }.to_json,
        timeout: 10
      )
      
      return default_movie_data(title, year, language) unless response.success?
      
      results = JSON.parse(response.body)
      extract_movie_info_from_results(title, results, year, language)
    rescue => e
      @logger.error "API error for #{title}: #{e.message}"
      return default_movie_data(title, year, language)
    end
  end

  def default_movie_data(title, year = nil, language = "English")
    ai_data = generate_movie_data_with_ai(title, year, language)
    ai_data || {
      year: year || '2020',
      genre: 'Drama',
      director: 'Unknown Director',
      cast: generate_cast_with_ai(title, year, language),
      plot: "#{title} is an engaging movie with compelling storyline and great performances.",
      duration: '120 min',
      language: language,
      content_rating: 'U/A',
      poster_url: nil,
      watch_url: "https://shemaroome.com/movies/#{title.downcase.gsub(' ', '-')}"
    }
  end



  def extract_movie_info_from_results(title, results, year = nil, language = "English")
    # Extract from search results
    text = results.dig('organic')&.first(3)&.map { |r| r['snippet'] }&.join(' ') || ''
    cast_info = extract_cast_with_characters(text, title, year, language)
    
    {
      year: year || extract_year(text),
      genre: extract_genre(text),
      director: extract_director(text, title),
      cast: cast_info,
      plot: extract_plot(text, title),
      duration: extract_duration(text),
      language: language,
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

  def extract_cast_with_characters(text, title, year = nil, language = "English")
    cast_array = []
    
    # More specific patterns for cast extraction
    cast_patterns = [
      /(?:starring|stars?)\s+([^.!?]+)/i,
      /(?:cast|actors?)\s*:?\s*([^.!?]+)/i,
      /(?:featuring|with)\s+([^.!?]+)/i,
      /([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:as|plays)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/i
    ]
    
    actors = []
    
    cast_patterns.each do |pattern|
      matches = text.scan(pattern)
      matches.each do |match|
        if match.is_a?(Array) && match.length == 2
          # "Actor as Character" pattern
          cast_array << { real_name: match[0].strip, character_name: match[1].strip }
        else
          # Extract actor names from cast list
          names = match.to_s.split(/,|\sand\s|\s&\s|\swith\s/).map(&:strip)
          names.each do |name|
            clean_name = name.gsub(/^(and|with|also|including)\s+/i, '').strip
            if clean_name.match?(/^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$/) && clean_name.length > 4
              actors << clean_name
            end
          end
        end
      end
    end
    
    # Clean and filter actors
    actors = actors.uniq.reject { |name| 
      name.downcase.include?(title.downcase) || 
      name.match?(/^(the|and|with|by|in|of|a|an|also|including)$/i) ||
      name.length < 5 ||
      name.match?(/\d/) # Reject names with numbers
    }.first(4)
    
    # Add actors without character names
    existing_names = cast_array.map { |c| c[:real_name] }
    actors.each do |actor|
      unless existing_names.include?(actor)
        cast_array << { real_name: actor, character_name: nil }
      end
    end
    
    # If still no cast found, use AI as fallback
    if cast_array.empty?
      cast_array = generate_cast_with_ai(title, year || extract_year(text), language)
    else
      # Generate character names for actors without them
      missing_chars = cast_array.select { |c| c[:character_name].nil? }
      if missing_chars.any?
        char_names = generate_character_names(title, missing_chars.map { |c| c[:real_name] })
        if char_names
          missing_chars.each_with_index do |cast_entry, index|
            cast_entry[:character_name] = char_names[index] || "Character #{index + 1}"
          end
        end
      end
    end
    
    cast_array
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



  def generate_character_names(title, actor_names)
    return nil if actor_names.empty?
    
    prompt = "For the movie '#{title}', what character names do these actors play: #{actor_names.join(', ')}? Provide only character names, one per line."
    
    begin
      response = @bedrock.invoke_model({
        model_id: BEDROCK_MODEL_ID,
        content_type: 'application/json',
        body: {
          anthropic_version: 'bedrock-2023-05-31',
          max_tokens: 150,
          messages: [{ role: 'user', content: prompt }]
        }.to_json
      })
      
      result = JSON.parse(response.body.read)
      content = result.dig('content', 0, 'text') || ''
      
      content.split("\n").map(&:strip).reject(&:empty?)
    rescue => e
      @logger.error "Character name generation error: #{e.message}"
      nil
    end
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
      cast_member = movie_data[:cast]&.sample
      actor = cast_member.is_a?(Hash) ? cast_member[:real_name] : cast_member || 'the lead actor'
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
      where_to_watch: generate_where_to_watch(movie_data),
      quality: generate_quality(movie_data)
    }
  end

  def generate_quality(movie_data)
    prompt = "Determine the video quality for #{movie_data[:title]} (#{movie_data[:year]}) movie. Return only one of these options: HD, 4K, or UHD based on the movie's production year and typical quality standards."

    response = invoke_bedrock(prompt)
    response&.strip || "HD"
  end



  def generate_blogs
    @logger.info "Generating blog data..."
    
    MovieTheme.where(:status => "published",:business_group_id => "548343938", :app_ids => "350502978", :episode_type => "movie" ).offset(400).limit(100).to_a.each do |movie|

      @logger.info "Processing: #{movie}"
     year = movie.release_date_string.to_date.year
     lang = movie.language || "Hindi"
      
      movie_data = search_movie_details(movie.title, year, lang)
      movie_data[:title] = movie.title
      
      reviews = generate_reviews(movie_data)
      ai_content = enhance_with_ai(movie_data)


       blog = Blog.find_or_initialize_by(title: movie.title)
      blog.assign_attributes(
         theme: "movie",
         theme_id: movie.id.to_s,
         title: movie_data[:title],
         language: movie_data[:language],
         duration: movie_data[:duration],
         rating: movie_data[:content_rating],
         quality: ai_content[:quality] || "HD",
         director: movie_data[:director],
         reviews: reviews,
         synopsis: ai_content[:seo_synopsis] || movie_data[:plot],
         why_watch: ai_content[:why_watch],
         where_watch: ai_content[:where_to_watch],
         cast: movie_data[:cast],
         hash_tag: ai_content[:seo_hashtags] || []
     )

   if blog.new_record?
     @logger.info "Creating new blog for #{movie.title}"
   else
     @logger.info "Updating existing blog for #{movie.title}"
  end

   blog.save!
    @logger.info "blog db---------#{blog.inspect}"
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

  def generate_quality(movie_data)
    prompt = "Determine the video quality for #{movie_data[:title]} (#{movie_data[:year]}) movie. Return only one of these options: HD, 4K, or UHD based on the movie's production year and typical quality standards."
    
    response = invoke_bedrock(prompt)
    response&.strip || "HD"
  end

  def generate_character_names(title, real_names)
    prompt = "For the movie '#{title}', generate character names for these actors: #{real_names.join(', ')}. Return only character names separated by commas, no explanations."
    
    response = invoke_bedrock(prompt)
    response&.split(',')&.map(&:strip) || []
  end

  def generate_cast_with_ai(title, year = nil, language = "English")
    prompt = "List the main cast of the #{language} movie '#{title}'#{year ? " (#{year})" : ''}. Provide only real actor names in this format: Actor Name|Character Name. Maximum 4 actors."
    
    response = invoke_bedrock(prompt)
    return default_cast if response.nil?
    
    cast_array = []
    response.split("\n").each do |line|
      if line.include?('|')
        parts = line.split('|').map(&:strip)
        cast_array << { real_name: parts[0], character_name: parts[1] }
      elsif line.match?(/^[A-Z][a-z]+\s+[A-Z][a-z]+/)
        cast_array << { real_name: line.strip, character_name: nil }
      end
    end
    
    cast_array.any? ? cast_array : default_cast
  end

  def default_cast
    [
      { real_name: 'Actor 1', character_name: 'Character 1' },
      { real_name: 'Actor 2', character_name: 'Character 2' },
      { real_name: 'Actor 3', character_name: 'Character 3' }
    ]
  end

  def generate_movie_data_with_ai(title, year = nil, language = "English")
    prompt = "Generate realistic movie data for the #{language} movie '#{title}'#{year ? " (#{year})" : ''}. Return in JSON format with these exact keys: year, genre, director, plot, duration, language, content_rating. Example: {\"year\": \"2023\", \"genre\": \"Action\", \"director\": \"John Smith\", \"plot\": \"A thrilling story...\", \"duration\": \"120 min\", \"language\": \"English\", \"content_rating\": \"PG-13\"}"
    
    response = invoke_bedrock(prompt)
    return nil unless response
    
    begin
      data = JSON.parse(response)
      cast = generate_cast_with_ai(title, year, language)
      
      {
        year: year || data['year'] || '2020',
        genre: data['genre'] || 'Drama',
        director: data['director'] || 'Unknown Director',
        cast: cast,
        plot: data['plot'] || "#{title} is an engaging movie with compelling storyline and great performances.",
        duration: data['duration'] || '120 min',
        language: language || data['language'] || 'English',
        content_rating: data['content_rating'] || 'U/A',
        poster_url: nil,
        watch_url: "https://shemaroome.com/movies/#{title.downcase.gsub(' ', '-')}"
      }
    rescue JSON::ParserError
      nil
    end
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
