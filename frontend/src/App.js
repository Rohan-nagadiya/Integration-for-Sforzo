import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';
import Asynchronous from './components/search';
function App() {
  const onRefresh = ()=>{
    return window.location.reload()
  }
  return (
    <div>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Patient Data
          </Typography>
          <Button color="inherit" onClick={onRefresh}>Refresh</Button>
        </Toolbar>
      </AppBar>
      {/* <Container sx={{ marginTop: '2rem' }}> */}
        <Asynchronous />
       
      {/* </Container> */}
    </div>
  );
}

export default App;
