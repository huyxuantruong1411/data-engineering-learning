# run_conservative.py
"""
Runner script for conservative pipeline - designed for anime-planet success
"""
import argparse
import logging
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.pipeline_conservative import run_conservative_pipeline

def main():
    parser = argparse.ArgumentParser(
        description="Conservative manga data fetcher - better for anime-planet"
    )
    parser.add_argument("--limit", type=int, default=5, 
                       help="Number of documents to process (default: 5)")
    parser.add_argument("--skip", type=int, default=0, 
                       help="Number of documents to skip")
    parser.add_argument("--only", nargs="*", 
                       choices=["mal", "anilist", "mangaupdates", "animeplanet"],
                       help="Only run for specific sources")
    parser.add_argument("--animeplanet-only", action="store_true",
                       help="Only run anime-planet (shortcut)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('manga_fetch_conservative.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    # Determine sources to run
    if args.animeplanet_only:
        only_sources = ["animeplanet"]
    else:
        only_sources = args.only
    
    logger.info(f"🚀 Starting conservative pipeline")
    logger.info(f"📋 Settings: limit={args.limit}, skip={args.skip}, only={only_sources}")
    
    if only_sources and "animeplanet" in only_sources:
        logger.warning("⚠️ Running anime-planet - this will be SLOW but more reliable")
        logger.warning("⚠️ Expected time: ~30-60 seconds per manga")
    
    try:
        results = run_conservative_pipeline(
            limit=args.limit, 
            skip=args.skip, 
            only=only_sources
        )
        
        print(f"\n{'='*80}")
        print("📊 CONSERVATIVE PIPELINE RESULTS")  
        print(f"{'='*80}")
        
        # Group results by source
        by_source = {}
        for result in results:
            source = result.get("source", "unknown")
            if source not in by_source:
                by_source[source] = []
            by_source[source].append(result)
        
        # Print summary by source
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
                
                # Success if we got any data
                if status in ["ok", "no_reviews"] or reviews > 0 or recs > 0:
                    success_count += 1
                    
                total_reviews += reviews
                total_recs += recs
                
                # Status emoji
                if status == "ok":
                    emoji = "✅" 
                elif status == "no_reviews":
                    emoji = "⚠️"
                elif status == "error":
                    emoji = "❌"
                else:
                    emoji = "❓"
                    
                print(f"  {emoji} {_id:25} | R:{reviews:3} | Rec:{recs:3} | {status}")
            
            success_rate = (success_count / len(source_results)) * 100 if source_results else 0
            print(f"  📈 Success: {success_count}/{len(source_results)} ({success_rate:.1f}%)")
            print(f"  📊 Total: {total_reviews} reviews, {total_recs} recommendations")
        
        print(f"\n🎉 Total results: {len(results)}")
        
        # Special anime-planet analysis
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
                  f"({(working_count/len(ap_results)*100):.1f}%)")
    
    except KeyboardInterrupt:
        logger.info("❌ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()