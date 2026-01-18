"""Job classification utility for categorizing and tagging jobs"""

import re
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field


# Job Categories
CATEGORIES = {
    "Frontend": {
        "title_keywords": [
            "frontend", "front-end", "front end", "frontend", "ui developer", "ui engineer",
            "react developer", "vue developer", "angular developer", "web developer",
            "javascript developer", "typescript developer"
        ],
        "description_keywords": [
            "frontend", "front-end", "user interface", "ui/ux", "responsive design",
            "css", "html", "dom", "browser"
        ]
    },
    "Backend": {
        "title_keywords": [
            "backend", "back-end", "back end", "server-side", "api developer",
            "python developer", "java developer", "golang developer", "go developer",
            "node developer", "nodejs developer", ".net developer", "c# developer"
        ],
        "description_keywords": [
            "backend", "back-end", "server-side", "api", "microservices",
            "database", "sql", "rest api", "graphql"
        ]
    },
    "Fullstack": {
        "title_keywords": [
            "fullstack", "full-stack", "full stack", "software engineer",
            "software developer", "web developer"
        ],
        "description_keywords": [
            "fullstack", "full-stack", "full stack", "end-to-end",
            "frontend and backend", "front-end and back-end"
        ]
    },
    "DevOps": {
        "title_keywords": [
            "devops", "sre", "site reliability", "platform engineer",
            "infrastructure engineer", "cloud engineer", "systems engineer",
            "devsecops", "mlops", "dataops"
        ],
        "description_keywords": [
            "devops", "ci/cd", "continuous integration", "continuous deployment",
            "infrastructure as code", "terraform", "ansible", "kubernetes",
            "docker", "containerization", "orchestration", "monitoring"
        ]
    },
    "AI": {
        "title_keywords": [
            "ai engineer", "ml engineer", "machine learning", "data scientist",
            "deep learning", "nlp engineer", "computer vision", "llm engineer",
            "ai/ml", "artificial intelligence", "generative ai", "genai"
        ],
        "description_keywords": [
            "machine learning", "deep learning", "neural network", "tensorflow",
            "pytorch", "nlp", "natural language processing", "computer vision",
            "llm", "large language model", "generative ai", "transformers",
            "hugging face", "openai", "langchain", "langgraph", "rag",
            "retrieval augmented", "fine-tuning", "model training"
        ]
    }
}

# Tags definitions grouped by type
TAGS = {
    # Programming Languages
    "languages": {
        "Python": ["python", "py"],
        "JavaScript": ["javascript", "js"],
        "TypeScript": ["typescript", "ts"],
        "Java": ["java", "jvm"],
        "Go": ["golang", "go"],
        "C++": ["c++", "cpp"],
        "C#": ["c#", "csharp", ".net"],
        "Rust": ["rust", "rustlang"],
        "Ruby": ["ruby", "rails"],
        "PHP": ["php", "laravel"],
        "Scala": ["scala"],
        "Kotlin": ["kotlin"],
        "Swift": ["swift"],
    },
    # Cloud Platforms
    "cloud": {
        "AWS": ["aws", "amazon web services", "ec2", "s3", "lambda", "dynamodb", "eks", "ecs"],
        "Azure": ["azure", "microsoft azure", "azure devops"],
        "GCP": ["gcp", "google cloud"],
        "Vercel": ["vercel"],
        "Cloudflare": ["cloudflare", "cloudflare workers"],
    },
    # Frameworks & Libraries
    "frameworks": {
        "React": ["react", "reactjs", "react.js"],
        "Next.js": ["next.js", "nextjs", "next js"],
        "Vue": ["vue", "vuejs", "vue.js", "nuxt"],
        "Angular": ["angular", "angularjs"],
        "Svelte": ["svelte", "sveltekit"],
        "FastAPI": ["fastapi", "fast api"],
        "Django": ["django"],
        "Flask": ["flask"],
        "Express": ["express", "expressjs", "express.js"],
        "NestJS": ["nestjs", "nest.js"],
        ".NET": [".net", "dotnet", "asp.net"],
        "Spring Boot": ["spring boot", "springboot", "spring framework"],
        "Rails": ["ruby on rails", "rails"],
        "Laravel": ["laravel"],
    },
    # AI/ML Tools
    "ai_tools": {
        "LangChain": ["langchain", "lang chain"],
        "LangGraph": ["langgraph", "lang graph"],
        "TensorFlow": ["tensorflow", "tf"],
        "PyTorch": ["pytorch", "torch"],
        "Hugging Face": ["hugging face", "huggingface", "transformers"],
        "OpenAI": ["openai", "gpt-4", "gpt-3", "chatgpt"],
        "LlamaIndex": ["llamaindex", "llama index"],
        "Scikit-learn": ["scikit-learn", "sklearn"],
        "Pandas": ["pandas"],
        "NumPy": ["numpy"],
    },
    # Databases
    "databases": {
        "PostgreSQL": ["postgresql", "postgres", "psql"],
        "MySQL": ["mysql"],
        "MongoDB": ["mongodb", "mongo"],
        "Redis": ["redis"],
        "Elasticsearch": ["elasticsearch", "elastic search", "elk"],
        "DynamoDB": ["dynamodb"],
        "Cassandra": ["cassandra"],
    },
    # DevOps Tools
    "devops_tools": {
        "Docker": ["docker", "containerization"],
        "Kubernetes": ["kubernetes", "k8s"],
        "Terraform": ["terraform", "iac"],
        "Ansible": ["ansible"],
        "Jenkins": ["jenkins"],
        "GitHub Actions": ["github actions", "gh actions"],
        "GitLab CI": ["gitlab ci", "gitlab-ci"],
        "ArgoCD": ["argocd", "argo cd"],
        "Helm": ["helm", "helm charts"],
        "Prometheus": ["prometheus"],
        "Grafana": ["grafana"],
        "DataDog": ["datadog"],
    },
    # Other Popular Tech
    "other": {
        "GraphQL": ["graphql", "graph ql"],
        "REST API": ["rest api", "restful"],
        "gRPC": ["grpc", "g-rpc"],
        "Kafka": ["kafka", "apache kafka"],
        "RabbitMQ": ["rabbitmq", "rabbit mq"],
        "WebSocket": ["websocket", "websockets", "ws"],
        "Microservices": ["microservices", "micro-services"],
        "Serverless": ["serverless", "faas"],
        "Agile": ["agile", "scrum", "kanban"],
        "Git": ["git", "github", "gitlab", "bitbucket"],
    }
}


@dataclass
class ClassificationResult:
    """Result of job classification"""
    category: str
    tags: List[str] = field(default_factory=list)
    confidence: float = 0.0


def _normalize_text(text: str) -> str:
    """Normalize text for matching"""
    return text.lower().strip()


def _match_keywords(text: str, keywords: List[str]) -> int:
    """
    Count keyword matches in text
    
    Args:
        text: Text to search in (should be lowercase)
        keywords: List of keywords to match
    
    Returns:
        Number of keyword matches found
    """
    matches = 0
    for keyword in keywords:
        # Check if keyword is a regex pattern
        if keyword.startswith(r"\b"):
            if re.search(keyword, text, re.IGNORECASE):
                matches += 1
        elif keyword.lower() in text:
            matches += 1
    return matches


def classify_job(title: str, description: str) -> ClassificationResult:
    """
    Classify a job based on title and description
    
    Args:
        title: Job title
        description: Job description
    
    Returns:
        ClassificationResult with category and tags
    """
    title_lower = _normalize_text(title)
    description_lower = _normalize_text(description)
    combined_text = f"{title_lower} {description_lower}"
    
    # Determine category
    category_scores: Dict[str, float] = {}
    
    for category, keywords in CATEGORIES.items():
        title_matches = _match_keywords(title_lower, keywords["title_keywords"])
        desc_matches = _match_keywords(description_lower, keywords["description_keywords"])
        
        # Title matches are weighted more heavily (3x)
        score = (title_matches * 3) + desc_matches
        category_scores[category] = score
    
    # Get the category with highest score
    best_category = max(category_scores, key=category_scores.get)
    best_score = category_scores[best_category]
    
    # Default to "Fullstack" if no strong match (score < 1)
    if best_score < 1:
        best_category = "Fullstack"
        confidence = 0.3
    else:
        # Normalize confidence (cap at 1.0)
        confidence = min(best_score / 10.0, 1.0)
    
    # Extract tags
    tags = extract_tags(title, description)
    
    return ClassificationResult(
        category=best_category,
        tags=tags,
        confidence=confidence
    )


def extract_tags(title: str, description: str) -> List[str]:
    """
    Extract technology tags from job title and description
    
    Args:
        title: Job title
        description: Job description
    
    Returns:
        List of matched tags
    """
    combined_text = _normalize_text(f"{title} {description}")
    matched_tags = []
    
    for tag_group, tags in TAGS.items():
        for tag_name, keywords in tags.items():
            for keyword in keywords:
                # Check if keyword is a regex pattern
                if keyword.startswith(r"\b"):
                    if re.search(keyword, combined_text, re.IGNORECASE):
                        if tag_name not in matched_tags:
                            matched_tags.append(tag_name)
                        break
                elif keyword.lower() in combined_text:
                    if tag_name not in matched_tags:
                        matched_tags.append(tag_name)
                    break
    
    return sorted(matched_tags)


def get_all_categories() -> List[str]:
    """Get list of all available categories"""
    return list(CATEGORIES.keys())


def get_all_tags() -> Dict[str, List[str]]:
    """Get all tags grouped by type"""
    result = {}
    for tag_group, tags in TAGS.items():
        result[tag_group] = list(tags.keys())
    return result


def get_flat_tags() -> List[str]:
    """Get flat list of all tag names"""
    all_tags = []
    for tag_group, tags in TAGS.items():
        all_tags.extend(tags.keys())
    return sorted(all_tags)
