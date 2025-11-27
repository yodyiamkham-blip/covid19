import subprocess
import sys
import time
 
def run_script(script_name):
    print(f"\n==========================")
    print(f" Running {script_name} ...")
    print(f"==========================\n")
    result = subprocess.run([sys.executable, script_name])
    if result.returncode != 0:
        print(f"❌ ERROR while running {script_name}")
        sys.exit(1)
    else:
        print(f"✔ Completed: {script_name}\n")
    time.sleep(1)
 
def main():
    print("\n🚀 Starting Full ETL Pipeline...")
    print("================================\n")
 
    run_script("ingest.py")
 
    run_script("transform.py")
 
    run_script("publish_batch_v2.py")
 
    print("\n🎉 All steps completed successfully!")
    print("🎉 Full pipeline run finished!")
 
if __name__ == "__main__":
    main()
    