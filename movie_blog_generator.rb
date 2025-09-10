#!/usr/bin/env ruby

require 'mongoid'
require 'httparty'
require 'nokogiri'
require 'json'
require 'logger'
require 'aws-sdk-bedrockruntime'

# Configure Mongoid
Mongoid.load!('mongoid.yml', :development)

# Movie model - read-only
class Movie
  include Mongoid::Document
  
  field :movie_id, type: String
  field :title, type: String
  field :year, type: String
  field :genre, type: String
  field :duration, type: String
  field :language, type: String
  field :content_rating, type: String
  field :plot, type: String
  field :cast, type: Array, default: []
  field :director, type: String
  field :poster_url, type: String
  field :watch_url, type: String
end

class MovieBlogGenerator
  BEDROCK_MODEL_ID = 'us.anthropic.claude-3-5-haiku-20241022-v1:0'
  
  def initialize
    @logger = Logger.new(STDOUT)
    @bedrock = Aws::BedrockRuntime::Client.new(region: 'us-east-1')
  end

  def generate_reviews(movie)
    reviews = []
    3.times do |i|
      reviews << {
        author: "Reviewer#{i+1}",
        rating: rand(2.0..5.0).round(1),
        content: "Great #{movie.genre} movie with excellent performances.",
        date: rand(30).days.ago
      }
    end
    reviews
  end

  def enhance_with_ai(movie)
    {
      why_watch: generate_why_watch(movie),
      seo_hashtags: generate_hashtags(movie),
      seo_synopsis: rewrite_synopsis(movie)
    }
  end

  def generate_blogs
    @logger.info "Generating blog pages..."
    
    timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
    blog_dir = "movie_blogs_#{timestamp}"
    Dir.mkdir(blog_dir) unless Dir.exist?(blog_dir)
    
    Movie.each do |movie|
      reviews = generate_reviews(movie)
      ai_content = enhance_with_ai(movie)
      
      html = build_movie_blog(movie, reviews, ai_content)
      filename = "#{movie.title.parameterize}-#{movie.year}.html"
      File.write("#{blog_dir}/#{filename}", html)
      
      @logger.info "Generated blog for: #{movie.title}"
      sleep(2)
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

  def generate_why_watch(movie)
    prompt = "Write a compelling 'Why You Should Watch' section for #{movie.title} (#{movie.year}). Genre: #{movie.genre}. Plot: #{movie.plot}. Keep it 150-200 words, engaging and professional."
    
    invoke_bedrock(prompt)
  end

  def generate_hashtags(movie)
    prompt = "Generate 15-20 SEO hashtags for #{movie.title} (#{movie.year}) #{movie.genre} movie for ShemarooMe platform. Return only hashtags separated by spaces."
    
    response = invoke_bedrock(prompt)
    response&.split&.select { |tag| tag.start_with?('#') }&.first(20)
  end

  def rewrite_synopsis(movie)
    prompt = "Rewrite this movie synopsis to be SEO-friendly and engaging: #{movie.plot}. Movie: #{movie.title} (#{movie.year}). Keep it 50-100 words."
    
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

  def build_movie_blog(movie, reviews, ai_content)
    reviews_html = reviews.map do |review|
      "<div class='review'><h4>#{review[:author]} (#{review[:rating]}/5)</h4><p>#{review[:content]}</p></div>"
    end.join

    <<~HTML
      <!DOCTYPE html>
      <html>
      <head>
        <title>#{movie.title} (#{movie.year})</title>
        <meta name="description" content="Watch #{movie.title} (#{movie.year}) - #{movie.genre} movie">
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-100">
        <div class="container mx-auto p-8">
          <h1 class="text-4xl font-bold mb-4">#{movie.title} (#{movie.year})</h1>
          <div class="grid md:grid-cols-2 gap-8">
            <div>
              #{movie.poster_url ? "<img src='#{movie.poster_url}' class='rounded-lg shadow-lg'>" : ''}
            </div>
            <div>
              <p class="mb-4"><strong>Genre:</strong> #{movie.genre}</p>
              <p class="mb-4"><strong>Director:</strong> #{movie.director}</p>
              <p class="mb-4"><strong>Cast:</strong> #{movie.cast.join(', ')}</p>
              <div class="mb-6">
                <h2 class="text-2xl font-bold mb-2">Synopsis</h2>
                <p>#{ai_content[:seo_synopsis] || movie.plot}</p>
              </div>
              #{ai_content[:why_watch] ? "<div class='mb-6'><h2 class='text-2xl font-bold mb-2'>Why You Should Watch</h2><p>#{ai_content[:why_watch]}</p></div>" : ''}
              <a href="#{movie.watch_url}" class="bg-blue-600 text-white px-6 py-3 rounded-lg">Watch Now</a>
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
      "<div class='movie-card p-4 border rounded'><h3><a href='#{movie.title.parameterize}-#{movie.year}.html'>#{movie.title} (#{movie.year})</a></h3><p>#{movie.genre}</p></div>"
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