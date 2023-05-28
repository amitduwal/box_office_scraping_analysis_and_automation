import scrapy

class BoxSpider(scrapy.Spider):
    name = "boxoffice"
    base_url = "https://www.boxofficemojo.com"
    years = ['2023']

    def start_requests(self):
        """
        Generate scrapy.Request objects for each URL based on the provided years.

        This method generates a list of URLs based on the years provided and yields a scrapy.Request
        object for each URL, with the specified callback function for further processing.

        Returns:
            A generator of scrapy.Request objects.

        Example usage:
            years = [2020, 2021, 2022]
        """
        urls = []
        for year in self.years:
            urls.append(f"{self.base_url}/year/world/{year}/")
        for url in urls:
            yield scrapy.Request(url=url,callback=self.parse)
    
    def parse(self,response):
        """
        Parse the response and extract movie information.

        This method is the callback function used to process the response from the URLs
        generated in `start_requests`. It extracts movie information from the response
        using CSS selectors, prints the number of movies found on the page, and follows
        the links to each movie's detail page.

        Args:
            response: The response object containing the HTML content of the page.

        Yields:
            scrapy.Request: A scrapy.Request object for each movie's detail page.

        
        """
        movies = response.css('td.a-text-left')
        print(f"Total {len(movies)} movies found in this page")


        for link in movies.css('a.a-link-normal'):
            href = link.css('::attr(href)').get()
            if not href:
                continue
            yield response.follow(self.base_url+href, self.parse_movie1)

        

    def parse_movie1(self,response):
        """
        Parse the response and extract movie information.

        This method is the callback function used to process the response from the URLs
        generated in `start_requests`. It extracts movie information from the response
        using CSS selectors, prints the number of movies found on the page, and follows
        the links to each movie's detail page.

        Args:
            response: The response object containing the HTML content of the page.

        Yields:
            scrapy.Request: A scrapy.Request object for each movie's detail page.

        
        """
        dom = response.css("table.mojo-table th.a-size-medium::text").get() 
        if dom == "Domestic":
            href = response.css("table.mojo-table a.a-link-normal::attr(href)").get()
            if href:
                yield response.follow(self.base_url+href, self.parse_movie2)

    def parse_movie2(self, response):
        

        title = response.css("h1.a-size-extra-large::text").get()
        description = response.css("p.a-size-medium::text").get() 

        money =  response.css("div.mojo-performance-summary-table").css("span.money::text").getall() 
        domesticBO = money[0]
        internationalBO = money[1]
        worldwideBO = money[2]

        distributor = response.css('div.mojo-summary-values div.a-section:nth-child(1) span:nth-child(2)::text').get()
        opening_amount = response.css('div.mojo-summary-values div.a-section:nth-child(2) span.money::text').get()
        release_date = response.css('div.mojo-summary-values div.a-section:nth-child(3) span a::text').get()
        mpaa_rating = response.css('div.mojo-summary-values div.a-section:nth-child(4) span:nth-child(2)::text').get()
        running_time = response.css('div.mojo-summary-values div.a-section:nth-child(5) span:nth-child(2)::text').get()
        genres = response.css('div.mojo-summary-values div.a-section:nth-child(6) span:nth-child(2)::text').getall()
        in_release = response.css('div.mojo-summary-values div.a-section:nth-child(7) span:nth-child(2)::text').get()
        widest_release = response.css('div.mojo-summary-values div.a-section:nth-child(8) span:nth-child(2)::text').get()

        mpaa = response.css('div.mojo-summary-values div.a-section:nth-child(4) span:nth-child(1)::text').get() 
        opening = response.css('div.mojo-summary-values div.a-section:nth-child(2) span::text').get()
        budget = response.css('div.mojo-summary-values div.a-section:nth-child(3) span:nth-child(1)::text').get()
        

        if mpaa == "MPAA":
            #for american domestic releases
            yield {
                "Title":title,
                "Description": description,
                "Domestic_BO": domesticBO,
                "International_BO": internationalBO,
                "Worldwide_BO": worldwideBO,
                'Distributor': distributor,
                'Opening_amount': opening_amount,
                'Release_date': release_date,
                'Mpaa_rating': mpaa_rating,
                'Running_time': running_time,
                'Genres': [genre.strip() for genre in genres[0].split('\n') if genre.strip()],
                'In_release': in_release,
                'Widest_release': widest_release,
            }
        elif budget == 'Budget':
            mpaa = response.css('div.mojo-summary-values div.a-section:nth-child(5) span:nth-child(1)::text').get()
            budget_amt = response.css('div.mojo-summary-values div.a-section:nth-child(3) span.money::text').get()
            release_date = response.css('div.mojo-summary-values div.a-section:nth-child(4) span a::text').get()
            if mpaa == "MPAA":
                # eg titanic

                
                wid = response.css('div.mojo-summary-values div.a-section:nth-child(9) span:nth-child(2)::text').get()
                if isinstance(genres, list):
                    pass
                else:
                    genres = genres[0]

                yield {
                    "Title":title,
                    "Description": description,
                    "Domestic_BO": domesticBO,
                    "International_BO": internationalBO,
                    "Worldwide_BO": worldwideBO,
                    'Distributor': distributor,
                    'Opening_amount': opening_amount,
                    'Budget': budget_amt,
                    'Release_date': release_date,
                    'Mpaa_rating': running_time,
                    'Running_time': genres,
                    'Genres': [genre.strip() for genre in in_release.split('\n') if genre.strip()],
                    'In_release': widest_release,
                    'Widest_release': wid,
                }
            else:
                yield {
                "Title":title,
                "Description": description,
                "Domestic_BO": domesticBO,
                "International_BO": internationalBO,
                "Worldwide_BO": worldwideBO,
                'Distributor': distributor,
                'Opening_amount': opening_amount,
                'Budget': budget_amt,
                'Release_date': release_date,
                
                'Running_time': running_time,
                'Genres': [genre.strip() for genre in genres[0].split('\n') if genre.strip()],
                'In_release': in_release,
                'Widest_release': widest_release,
            }
        elif opening != 'Opening':
            #eg. The Wandering Earth II
            release_date = response.css('div.mojo-summary-values div.a-section:nth-child(2) span a::text').get()
            running_time = response.css('div.mojo-summary-values div.a-section:nth-child(3) span:nth-child(2)::text').get()
            in_release = response.css('div.mojo-summary-values div.a-section:nth-child(5) span:nth-child(2)::text').get()

            yield {
                "Title":title,
                "Description": description,
                "Domestic_BO": domesticBO,
                "International_BO": internationalBO,
                "Worldwide_BO": worldwideBO,
                'Distributor': distributor,
                'Release_date': release_date,
                'Running_time': running_time,
                'Genres': [genre.strip() for genre in mpaa_rating.split('\n') if genre.strip()],
                'In_release': in_release,
                'Widest_release': genres[0],
            }
        elif mpaa == "Genres":

            # The Journey: A Music Special from Andrea Bocelli
            yield {
                "Title":title,
                "Description": description,
                "Domestic_BO": domesticBO,
                "International_BO": internationalBO,
                "Worldwide_BO": worldwideBO,
                'Distributor': distributor,
                'Opening_amount': opening_amount,
                

                'Release_date': release_date,
                'Genres': mpaa_rating,
                'In_release': running_time,
                'Widest_release': genres[0],
            }
        else:
            yield {
                "Title":title,
                "Description": description,
                "Domestic_BO": domesticBO,
                "International_BO": internationalBO,
                "Worldwide_BO": worldwideBO,
                'Distributor': distributor,
                'Opening_amount': opening_amount,
                'Release_date': release_date,

                'Running_time': mpaa_rating,
                'Genres': [genre.strip() for genre in running_time.split('\n') if genre.strip()],
                'In_release': genres[0],
                'Widest_release': in_release,
            }
