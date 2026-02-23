# Smart Task Assignment System

A modern web application for intelligent task assignment and employee management, built with FastAPI backend and React frontend.

## 🚀 Features

### ✅ **Implemented Features**

#### **Backend (FastAPI)**
- **Employee Management**
  - Get all employees with complete profiles
  - Group employees by team leads (10 teams, 88 employees)
  - Employee search and filtering

- **Smart Recommendations**
  - AI-powered task recommendations based on skills
  - Match scoring algorithm (0-15+ range)
  - Top 10 employee matches for any task

- **Task Management**
  - Assign tasks to employees
  - Track task status (OPEN, ASSIGNED, IN_PROGRESS, DONE)
  - Task history and analytics

- **Permit File Management**
  - PDF file upload and storage
  - File metadata tracking
  - Status workflow management

#### **Frontend (React + TypeScript)**
- **Dashboard**
  - Real-time statistics
  - Recent activities feed
  - Quick action buttons

- **Employee Directory**
  - Team-based organization
  - Advanced search and filtering
  - Detailed employee profiles

- **Smart Recommender**
  - Team selection interface
  - Task description input
  - File upload integration
  - Recommendation display with scores

- **Task Board**
  - Kanban-style task management
  - Drag-and-drop interface
  - Status-based columns

- **Permit Files**
  - File listing with status badges
  - Detailed file view
  - Upload and management

## 📁 Project Structure

```
task_assignee/
├── app/                     # FastAPI Backend
│   ├── api/v1/routers/     # API endpoints
│   ├── core/               # Settings and configuration
│   └── main.py            # Application entry point
├── frontend/               # React Frontend
│   ├── src/
│   │   ├── pages/         # Application pages
│   │   ├── components/    # Reusable components
│   │   └── lib/           # API client and utilities
│   └── package.json
├── data/seed/             # JSON data files
├── docs/                  # Documentation
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## 🛠️ Tech Stack

### **Backend**
- **FastAPI** - Modern Python web framework
- **Python 3.12** - Programming language
- **JSON Storage** - Temporary data storage
- **CORS** - Cross-origin resource sharing

### **Frontend**
- **React 18** - UI framework
- **TypeScript** - Type-safe JavaScript
- **Vite** - Build tool and dev server
- **Tailwind CSS** - Utility-first CSS
- **shadcn/ui** - Component library
- **Lucide React** - Icon library

## 🚀 Quick Start

### **Prerequisites**
- Python 3.12+
- Node.js 18+
- npm or yarn

### **Backend Setup**
```bash
# Navigate to project
cd task_assignee

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start backend server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### **Frontend Setup**
```bash
# Navigate to frontend
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

### **Access the Application**
- Frontend: http://localhost:8080
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## 📊 API Endpoints

### **Employees**
- `GET /api/v1/employees/` - Get all employees
- `GET /api/v1/employees/employees-grouped-by-team-lead` - Get employees grouped by team

### **Tasks**
- `POST /api/v1/recommendations/` - Get task recommendations
- `POST /api/v1/tasks/assign` - Assign task to employee

### **Permit Files**
- `GET /api/v1/permit-files/` - Get all permit files
- `POST /api/v1/permit-files/intake` - Upload new permit file

## 👥 Employee Data

The system includes **88 employees** organized into **10 teams**:

1. **Rahul K** - 5 members
2. **Gaurav Mavi** - 6 members
3. **Harish Kumar** - 6 members
4. **Prashant Sharma** - 13 members
5. **Rohan Kashid** - 8 members
6. **Sandeep Negi** - 2 members
7. **Saurav Yadav** - 6 members
8. **Shivam Kumar** - 7 members
9. **SunderRaj D** - 9 members
10. **Tanveer Alam** - 13 members

## 🎯 How to Use

### **1. View Dashboard**
- See active tasks and permit files count
- Monitor recent activities

### **2. Browse Employees**
- Navigate to Employee Directory
- View teams and their members
- Search and filter employees

### **3. Get Smart Recommendations**
- Go to AI Recommender
- Select a team lead
- Enter task description
- Click "Find Eligible Employees"
- View ranked recommendations with match scores

### **4. Assign Tasks**
- Choose from team members or recommended employees
- Enter task details
- Optionally upload permit file
- Assign task with confirmation

### **5. Manage Permit Files**
- Upload PDF files
- Track file status through workflow
- View detailed file information

## ⚠️ Current Limitations

### **Temporary Solutions**
- Data stored in JSON files (should use database)
- In-memory file storage (should use persistent storage)
- No user authentication (hardcoded employee code)

### **Not Yet Implemented**
- Real-time updates
- Email notifications
- Advanced analytics
- Mobile app

## 🔄 Future Development

### **Short Term**
- Database integration (PostgreSQL)
- User authentication system
- PDF content extraction
- Semantic search on documents

### **Medium Term**
- Real-time WebSocket updates
- Email notifications
- Advanced reporting
- Docker containerization

### **Long Term**
- Mobile application
- Enterprise features
- Multi-tenant support
- Advanced AI features

## 📝 Development Notes

- Backend runs on port 8000
- Frontend runs on port 8080
- All API calls are type-safe
- Components use modern React patterns
- Responsive design for all screen sizes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is proprietary and confidential.

---

**Last Updated**: January 2026
**Version**: 1.0.0
**Status**: Production Ready (MVP)