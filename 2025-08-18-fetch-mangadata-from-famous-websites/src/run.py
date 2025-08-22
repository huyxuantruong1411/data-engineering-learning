import argparse
import logging
import sys
import os

# Fix Python path for relative imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'spiders'))

from src.pipeline_conservative import run_conservative_pipeline as run_pipeline
from src.pipeline import run_mal_ranking_based_crawl
from scrapy.crawler import CrawlerProcess
from mal_manga_spider import MALMangaSpider

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('manga_fetch.log')
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Manga Data Pipeline")
    parser.add_argument("--limit", type=int, default=5, 
                       help="Number of documents to process (default: 5)")
    parser.add_argument("--skip", type=int, default=0, 
                       help="Number of documents to skip")
    parser.add_argument("--only", nargs="*", 
                       choices=["mal", "anilist", "mangaupdates", "animeplanet"],
                       help="Only run for specific sources")
    parser.add_argument("--animeplanet-only", action="store_true",
                       help="Only run anime-planet (shortcut)")
    parser.add_argument("--mal-manga-crawl", action="store_true",
                       help="Run MAL manga crawler spider to fetch top manga")
    parser.add_argument("--mal-ranking-crawl", action="store_true",
                       help="Run independent MAL ranking-based collection (like project_dump.txt)")
    parser.add_argument("--mal-start-limit", type=int, default=0,
                       help="Starting ranking limit for MAL collection (default: 0)")
    parser.add_argument('--mal-max-pages', type=int, default=50, 
                        help='Maximum number of ranking pages to process for MAL ranking crawl (0 = unlimited)')
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    if args.animeplanet_only:
        only_sources = ["animeplanet"]
    else:
        only_sources = args.only

    if args.mal_manga_crawl:
        logger.info("🚀 Starting MAL Manga Crawler Spider")
        try:
            process = CrawlerProcess()
            process.crawl(MALMangaSpider)
            process.start()
        except Exception as e:
            logger.error(f"Failed to run MAL Manga Spider: {e}", exc_info=True)
    elif args.mal_ranking_crawl:
        logger.info("🚀 Starting MAL Ranking-Based Collection (Independent Approach)")
        logger.info(f"📋 Settings: start_limit={args.mal_start_limit}, max_pages={args.mal_max_pages}")
        logger.warning("⚠️ This approach crawls MAL independently without external IDs")
        logger.warning("⚠️ Expected time: ~1 object/second for 24h target")
        try:
            results = run_mal_ranking_based_crawl(args.mal_start_limit, args.mal_max_pages)
            
            print(f"\n{'='*80}")
            print("📊 MAL RANKING-BASED COLLECTION RESULTS")
            print(f"{'='*80}")
            
            success_count = 0
            total_reviews = 0
            total_recs = 0
            
            for result in results:
                status = result.get("status", "unknown")
                reviews = len(result.get("reviews", []))
                recs = len(result.get("recommendations", []))
                
                if status in ["ok", "no_reviews"] or reviews > 0 or recs > 0:
                    success_count += 1
                    
                total_reviews += reviews
                total_recs += recs
            
            success_rate = (success_count / len(results)) * 100 if results else 0
            print(f"📈 Success: {success_count}/{len(results)} ({success_rate:.1f}%)")
            print(f"📊 Total: {total_reviews} reviews, {total_recs} recommendations")
            print(f"🎉 Total manga processed: {len(results)}")
            
        except Exception as e:
            logger.error(f"Failed to run MAL ranking-based collection: {e}", exc_info=True)
    else:
        logger.info(f"🚀 Starting manga data pipeline")
        logger.info(f"📋 Settings: limit={args.limit}, skip={args.skip}, only={only_sources}")

        if only_sources and "animeplanet" in only_sources:
            logger.warning("⚠️ Running anime-planet - this will be SLOW but more reliable")
            logger.warning("⚠️ Expected time: ~30-60 seconds per manga")

        try:
            results = run_pipeline(
                limit=args.limit, 
                skip=args.skip, 
                only=only_sources
            )
            
            print(f"\n{'='*80}")
            print("📊 PIPELINE RESULTS")  
            print(f"{'='*80}")
            
            by_source = {}
            for result in results:
                source = result.get("source", "unknown")
                if source not in by_source:
                    by_source[source] = []
                by_source[source].append(result)
            
            for source, source_results in by_source.items():
                print(f"\n🔹 {source.upper()}:")
                
                success_count = 0
                total_reviews = 0
                total_recs = 0
                
                for result in source_results:
                    _id = result.get("_id", "unknown")
                    status = result.get("status", "unknown")
                    reviews = len(result.get("reviews", []))
                    recs = len(result.get("recommendations", []))
                    
                    if status in ["ok", "no_reviews"] or reviews > 0 or recs > 0:
                        success_count += 1
                        
                    total_reviews += reviews
                    total_recs += recs
                    
                    emoji = "✅" if status == "ok" else "⚠️" if status == "no_reviews" else "❌" if status == "error" else "❓"
                    print(f"  {emoji} {_id:25} | R:{reviews:3} | Rec:{recs:3} | {status}")
                
                success_rate = (success_count / len(source_results)) * 100 if source_results else 0
                print(f"  📈 Success: {success_count}/{len(source_results)} ({success_rate:.1f}%)")
                print(f"  📊 Total: {total_reviews} reviews, {total_recs} recommendations")
            
            print(f"\n🎉 Total results: {len(results)}")
            
            ap_results = by_source.get("animeplanet", [])
            if ap_results:
                print(f"\n{'='*40}")
                print("🎯 ANIME-PLANET DETAILED ANALYSIS")
                print(f"{'='*40}")
                
                working_count = 0
                for result in ap_results:
                    reviews = len(result.get("reviews", []))
                    recs = len(result.get("recommendations", []))
                    main = result.get("main", {})
                    has_main = bool(main.get("title") or main.get("synopsis"))
                    
                    if reviews > 0 or recs > 0 or has_main:
                        working_count += 1
                        
                    print(f"  {result.get('source_id', 'unknown'):20} | "
                          f"Reviews:{reviews:3} | Recs:{recs:3} | "
                          f"Main:{has_main} | Method:{result.get('method', 'unknown')}")
                
                print(f"\n  🎯 Anime-Planet working rate: {working_count}/{len(ap_results)} "
                      f"({(working_count/len(ap_results)*100 if ap_results else 0):.1f}%)")
        
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    main()