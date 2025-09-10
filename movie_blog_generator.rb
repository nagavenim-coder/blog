#!/usr/bin/env ruby

require 'mongoid'
require 'httparty'
require 'nokogiri'
require 'json'
require 'logger'
require 'aws-sdk-bedrockruntime'

# Configure Mongoid
Mongoid.load!('mongoid.yml', :development)

# Movie model - read-only (only title)
class Movie
  include Mongoid::Document
  
  field :title, type: String
end

class MovieBlogGenerator
  BEDROCK_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
  SERPER_API_KEY = ENV['SERPER_API_KEY'] || 'YOUR_SERPER_API_KEY_HERE'
  
  def initialize
    @logger = Logger.new(STDOUT)
    @bedrock = Aws::BedrockRuntime::Client.new(region: 'us-east-1')
  end

  def search_movie_details(title)
    query = "#{title} movie plot cast director year genre"
    
    response = HTTParty.post('https://google.serper.dev/search',
      headers: {
        'X-API-KEY' => SERPER_API_KEY,
        'Content-Type' => 'application/json'
      },
      body: { q: query }.to_json
    )
    
    return {} unless response.success?
    
    results = JSON.parse(response.body)
    extract_movie_info_from_results(title, results)
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
      content_rating: 'PG-13',
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
    reviews = []
    3.times do |i|
      reviews << {
        author: "Reviewer#{i+1}",
        rating: rand(2.0..5.0).round(1),
        content: "Great #{movie_data[:genre]} movie with excellent performances by #{movie_data[:cast].first}.",
        date: rand(30).days.ago
      }
    end
    reviews
  end

  def enhance_with_ai(movie_data)
    {
      why_watch: generate_why_watch(movie_data),
      seo_hashtags: generate_hashtags(movie_data),
      seo_synopsis: rewrite_synopsis(movie_data)
    }
  end

  def generate_blogs
    @logger.info "Generating blog pages..."
    
    timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
    blog_dir = "movie_blogs_#{timestamp}"
    Dir.mkdir(blog_dir) unless Dir.exist?(blog_dir)
    
    Movie.each do |movie|
      @logger.info "Processing: #{movie.title}"
      
      # Search for movie details using Serper API
      movie_data = search_movie_details(movie.title)
      movie_data[:title] = movie.title
      
      reviews = generate_reviews(movie_data)
      ai_content = enhance_with_ai(movie_data)
      
      html = build_movie_blog(movie_data, reviews, ai_content)
      filename = "#{movie.title.parameterize}-#{movie_data[:year]}.html"
      File.write("#{blog_dir}/#{filename}", html)
      
      @logger.info "Generated blog for: #{movie.title}"
      sleep(3) # Rate limiting for API
    end
    
    # Generate index
    index_html = build_index_page(blog_dir)
    File.write("#{blog_dir}/index.html", index_html)
    
    @logger.info "All blogs saved in: #{blog_dir}"
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

  def build_movie_blog(movie_data, reviews, ai_content)
    reviews_html = reviews.map do |review|
      "<div class='review'><h4>#{review[:author]} (#{review[:rating]}/5)</h4><p>#{review[:content]}</p></div>"
    end.join

    <<~HTML
      <!DOCTYPE html>
      <html>
      <head>
        <title>#{movie_data[:title]} (#{movie_data[:year]})</title>
        <meta name="description" content="Watch #{movie_data[:title]} (#{movie_data[:year]}) - #{movie_data[:genre]} movie">
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-100">
        <div class="container mx-auto p-8">
          <h1 class="text-4xl font-bold mb-4">#{movie_data[:title]} (#{movie_data[:year]})</h1>
          <div class="grid md:grid-cols-2 gap-8">
            <div>
              #{movie_data[:poster_url] ? "<img src='#{movie_data[:poster_url]}' class='rounded-lg shadow-lg'>" : '<div class="bg-gray-200 h-96 rounded-lg flex items-center justify-center"><p class="text-gray-500">No Poster Available</p></div>'}
            </div>
            <div>
              <p class="mb-4"><strong>Genre:</strong> #{movie_data[:genre]}</p>
              <p class="mb-4"><strong>Director:</strong> #{movie_data[:director]}</p>
              <p class="mb-4"><strong>Cast:</strong> #{movie_data[:cast].join(', ')}</p>
              <div class="mb-6">
                <h2 class="text-2xl font-bold mb-2">Synopsis</h2>
                <p>#{ai_content[:seo_synopsis] || movie_data[:plot]}</p>
              </div>
              #{ai_content[:why_watch] ? "<div class='mb-6'><h2 class='text-2xl font-bold mb-2'>Why You Should Watch</h2><p>#{ai_content[:why_watch]}</p></div>" : ''}
              <a href="#{movie_data[:watch_url]}" class="bg-blue-600 text-white px-6 py-3 rounded-lg">Watch Now</a>
            </div>
          </div>
          <div class="mt-12">
            <h2 class="text-2xl font-bold mb-4">Reviews</h2>
            #{reviews_html}
          </div>
          #{ai_content[:seo_hashtags]&.any? ? "<div class='mt-8'><h3 class='text-xl font-bold mb-2'>Hashtags</h3><p>#{ai_content[:seo_hashtags].join(' ')}</p></div>" : ''}
        </div>
      </body>
      </html>
    HTML
  end

  def build_index_page(blog_dir)
    movies_html = Movie.all.map do |movie|
      "<div class='movie-card p-4 border rounded'><h3><a href='#{movie.title.parameterize}-2020.html'>#{movie.title}</a></h3><p>Movie Blog</p></div>"
    end.join

    <<~HTML
      <!DOCTYPE html>
      <html>
      <head>
        <title>ShemarooMe Movie Blog</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-100">
        <div class="container mx-auto p-8">
          <h1 class="text-4xl font-bold mb-8">ShemarooMe Movie Blog</h1>
          <div class="grid md:grid-cols-3 gap-6">
            #{movies_html}
          </div>
        </div>
      </body>
      </html>
    HTML
  end

end

# CLI interface
if __FILE__ == $0
  generator = MovieBlogGenerator.new
  generator.run_pipeline
end